/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
class MetadataCache {
    private final String clusterId;
    // 各个节点
    private final List<Node> nodes;
    // 未授权的 topics
    private final Set<String> unauthorizedTopics;
    // 暂不可用的 topics
    private final Set<String> invalidTopics;
    // 内部的 topics
    private final Set<String> internalTopics;
    // 控制器
    private final Node controller;
    // 映射TopicPartition --> PartitionInfoAndEpoch 关系
    private final Map<TopicPartition, PartitionInfoAndEpoch> metadataByPartition;
    // 集群信息
    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Cluster clusterInstance) {
        // 集群id,刚开始是null
        this.clusterId = clusterId;
        // 集群中的节点
        this.nodes = nodes;
        // 未授权的 topics
        this.unauthorizedTopics = unauthorizedTopics;
        // 不可用的 topics
        this.invalidTopics = invalidTopics;
        // 内部的topics
        this.internalTopics = internalTopics;
        // 控制器
        this.controller = controller;
        // 分区元数据
        this.metadataByPartition = new HashMap<>(partitions.size());
        for (PartitionInfoAndEpoch p : partitions) {
            this.metadataByPartition.put(new TopicPartition(p.partitionInfo().topic(), p.partitionInfo().partition()), p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    /**
     * Return the cached PartitionInfo iff it was for the given epoch
     */
    Optional<PartitionInfo> getPartitionInfoHavingEpoch(TopicPartition topicPartition, int epoch) {
        PartitionInfoAndEpoch infoAndEpoch = metadataByPartition.get(topicPartition);
        if (infoAndEpoch == null) {
            return Optional.empty();
        } else {
            if (infoAndEpoch.epoch() == epoch) {
                return Optional.of(infoAndEpoch.partitionInfo());
            } else {
                return Optional.empty();
            }
        }
    }

    Optional<PartitionInfo> getPartitionInfo(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition))
                .map(PartitionInfoAndEpoch::partitionInfo);
    }

    synchronized void retainTopics(Collection<String> topics) {
        metadataByPartition.entrySet().removeIf(entry -> !topics.contains(entry.getKey().topic()));
        unauthorizedTopics.retainAll(topics);
        invalidTopics.retainAll(topics);
        computeClusterView();
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }
    // 创建映射关系
    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(PartitionInfoAndEpoch::partitionInfo)
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }
    // 缓存集群的信息
    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        // 通过address 记录下所有的 地址
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        // 创建 metadata 缓存
        // Cluster.bootstrap 这里还使用给定的地址,创建了 Cluster,即集群信息
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyList(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "cluster=" + cluster() +
                '}';
    }

    static class PartitionInfoAndEpoch {
        private final PartitionInfo partitionInfo;
        private final int epoch;

        PartitionInfoAndEpoch(PartitionInfo partitionInfo, int epoch) {
            this.partitionInfo = partitionInfo;
            this.epoch = epoch;
        }

        public PartitionInfo partitionInfo() {
            return partitionInfo;
        }

        public int epoch() {
            return epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionInfoAndEpoch that = (PartitionInfoAndEpoch) o;
            return epoch == that.epoch &&
                    Objects.equals(partitionInfo, that.partitionInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionInfo, epoch);
        }

        @Override
        public String toString() {
            return "PartitionInfoAndEpoch{" +
                    "partitionInfo=" + partitionInfo +
                    ", epoch=" + epoch +
                    '}';
        }
    }
}
