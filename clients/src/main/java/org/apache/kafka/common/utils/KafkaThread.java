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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Thread that sets things up nicely
 */
public class KafkaThread extends Thread {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    public static KafkaThread daemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, true);
    }

    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, false);
    }

    public KafkaThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }
    // 发送的线程
    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        // 对此线程的一个配置
        configureThread(name, daemon);
    }
    // 对线程的配置
    private void configureThread(final String name, boolean daemon) {
        // 设置是否是 daemon 线程
        setDaemon(daemon);
        // 设置 未捕获异常处理
        setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '{}':", name, e);
            }
        });
    }

}
