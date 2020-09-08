/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.function.Supplier

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.Processor._
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.{KafkaException, Reconfigurable}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.metrics.stats.Total
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection._
import JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 *  - control-plane :
 *    - Handles requests from controller. This is optional and can be configured by specifying "control.plane.listener.name".
 *      If not configured, the controller requests are handled by the data-plane.
 *    - The threading model is
 *      1 Acceptor thread that handles new connections
 *      Acceptor has 1 Processor thread that has its own selector and read requests from the socket.
 *      1 Handler thread that handles requests and produce responses back to the processor thread for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {
  // queued.max.requests  最大请求数
  private val maxQueuedRequests = config.queuedMaxRequests

  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  // data-plane 处理器
  private val dataPlaneProcessors = new ConcurrentHashMap[Int, Processor]()
  // dataplane nio selector, endpoint和acceptor的映射关系
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  // 数据相关请求的 处理
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneMetricPrefix)
  // control-plane
  private var controlPlaneProcessorOpt : Option[Processor] = None
  private[network] var controlPlaneAcceptorOpt : Option[Acceptor] = None
  val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ => new RequestChannel(20, ControlPlaneMetricPrefix))

  private var nextProcessorId = 0
  private var connectionQuotas: ConnectionQuotas = _
  private var stoppedProcessingRequests = false

  /**
   * Start the socket server. Acceptors for all the listeners are started. Processors
   * are started if `startupProcessors` is true. If not, processors are only started when
   * [[kafka.network.SocketServer#startProcessors()]] is invoked. Delayed starting of processors
   * is used to delay processing client connections until server is fully initialized, e.g.
   * to ensure that all credentials have been loaded before authentications are performed.
   * Acceptors are always started during `startup` so that the bound port is known when this
   * method completes even when ephemeral ports are used. Incoming connections on this server
   * are processed when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startupProcessors Flag indicating whether `Processor`s must be started.
   */
  def startup(startupProcessors: Boolean = true) {
    this.synchronized {
      // 每个ip的连接限制
      connectionQuotas = new ConnectionQuotas(config.maxConnectionsPerIp, config.maxConnectionsPerIpOverrides)
      // control.plane.listener.name 的值
      // 此是 创建  控制器相关的 acceptor 和 processor
      createControlPlaneAcceptorAndProcessor(config.controlPlaneListener)
      // 此是 创建 data plane 即 数据处理相关的
      createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, config.dataPlaneListeners)
      if (startupProcessors) {
        startControlPlaneProcessor()
        startDataPlaneProcessors()
      }
    }

    newGauge(s"${DataPlaneMetricPrefix}NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val ioWaitRatioMetricNames = dataPlaneProcessors.values.asScala.map { p =>
            metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
          }
          ioWaitRatioMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
          }.sum / dataPlaneProcessors.size
        }
      }
    )
    newGauge(s"${ControlPlaneMetricPrefix}NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
            metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
          }
          ioWaitRatioMetricName.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
          }.getOrElse(Double.NaN)
        }
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    newGauge(s"${DataPlaneMetricPrefix}ExpiredConnectionsKilledCount",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.values.asScala.map { p =>
            metrics.metricName("expired-connections-killed-count", "socket-server-metrics", p.metricTags)
          }
          expiredConnectionsKilledCountMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
          }.sum
        }
      }
    )
    newGauge(s"${ControlPlaneMetricPrefix}ExpiredConnectionsKilledCount",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
            metrics.metricName("expired-connections-killed-count", "socket-server-metrics", p.metricTags)
          }
          expiredConnectionsKilledCountMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
          }.getOrElse(0.0)
        }
      }
    )
    info(s"Started ${dataPlaneAcceptors.size} acceptor threads for data-plane")
    if (controlPlaneAcceptorOpt.isDefined)
      info("Started control-plane acceptor thread")
  }

  /**
   * Starts processors of all the data-plane acceptors of this server if they have not already been started.
   * This method is used for delayed starting of data-plane processors if [[kafka.network.SocketServer#startup]]
   * was invoked with `startupProcessors=false`.
   */
  def startDataPlaneProcessors(): Unit = synchronized {
    dataPlaneAcceptors.values.asScala.foreach { _.startProcessors(DataPlaneThreadPrefix) }
    info(s"Started data-plane processors for ${dataPlaneAcceptors.size} acceptors")
  }

  /**
   * Start the processor of control-plane acceptor of this server if it has not already been started.
   * This method is used for delayed starting of control-plane processor if [[kafka.network.SocketServer#startup]]
   * was invoked with `startupProcessors=false`.
   */
  def startControlPlaneProcessor(): Unit = synchronized {
    controlPlaneAcceptorOpt.foreach { controlPlaneAcceptor =>
      controlPlaneAcceptor.startProcessors(ControlPlaneThreadPrefix)
      info(s"Started control-plane processor for the control-plane acceptor")
    }
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  private def createDataPlaneAcceptorsAndProcessors(dataProcessorsPerListener: Int,
                                                    endpoints: Seq[EndPoint]): Unit = synchronized {
    endpoints.foreach { endpoint =>
      // data nio selector 接收器
      val dataPlaneAcceptor = createAcceptor(endpoint, DataPlaneMetricPrefix)
      // 创建 processor
      addDataPlaneProcessors(dataPlaneAcceptor, endpoint, dataProcessorsPerListener)
      // 后台启动 此  nioselector
      KafkaThread.nonDaemon(s"data-plane-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}", dataPlaneAcceptor).start()
//      // 防止线程 退出
      dataPlaneAcceptor.awaitStartup()
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
      info(s"Created data-plane acceptor and processors for endpoint : $endpoint")
    }
  }
  // 创建控制器相关的 端口监听  和 处理器 processor
  private def createControlPlaneAcceptorAndProcessor(endpointOpt: Option[EndPoint]): Unit = synchronized {
    endpointOpt.foreach { endpoint =>
      // 创建 nio selector  serverSocketChannel
      val controlPlaneAcceptor = createAcceptor(endpoint, ControlPlaneMetricPrefix)
      // 创建 selector 对新接入的连接 处理
      val controlPlaneProcessor = newProcessor(nextProcessorId, controlPlaneRequestChannelOpt.get, connectionQuotas, endpoint.listenerName, endpoint.securityProtocol, memoryPool)
      controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
      controlPlaneProcessorOpt = Some(controlPlaneProcessor)
      val listenerProcessors = new ArrayBuffer[Processor]()
      listenerProcessors += controlPlaneProcessor
      controlPlaneRequestChannelOpt.foreach(_.addProcessor(controlPlaneProcessor))
      nextProcessorId += 1
      controlPlaneAcceptor.addProcessors(listenerProcessors, ControlPlaneThreadPrefix)
      // nio selector 启动
      KafkaThread.nonDaemon(s"${ControlPlaneThreadPrefix}-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}", controlPlaneAcceptor).start()
      // 防止线程 退出
      controlPlaneAcceptor.awaitStartup()
      info(s"Created control-plane acceptor and processor for endpoint : $endpoint")
    }
  }
  // 连接器
  private def createAcceptor(endPoint: EndPoint, metricPrefix: String) : Acceptor = synchronized {
    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId
    // 创建nioselector 以及 serverSocketChannel
    new Acceptor(endPoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas, metricPrefix)
  }

  private def addDataPlaneProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()
    for (_ <- 0 until newProcessorsPerListener) {
      // 创建 processor
      val processor = newProcessor(nextProcessorId, dataPlaneRequestChannel, connectionQuotas, listenerName, securityProtocol, memoryPool)
      // 记录此 processor
      listenerProcessors += processor
      //
      dataPlaneRequestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    // 记录id  和  processor的映射
    listenerProcessors.foreach(p => dataPlaneProcessors.put(p.id, p))
    // 记录此 processor 到 acceptor中,并启动
    acceptor.addProcessors(listenerProcessors, DataPlaneThreadPrefix)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests() = {
    info("Stopping socket server request processors")
    this.synchronized {
      dataPlaneAcceptors.asScala.values.foreach(_.shutdown())
      controlPlaneAcceptorOpt.foreach(_.shutdown())
      dataPlaneProcessors.asScala.values.foreach(_.shutdown())
      controlPlaneProcessorOpt.foreach(_.shutdown())
      dataPlaneRequestChannel.clear()
      controlPlaneRequestChannelOpt.foreach(_.clear())
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each data-plane listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      dataPlaneAcceptors.asScala.foreach { case (endpoint, acceptor) =>
        addDataPlaneProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      dataPlaneAcceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, dataPlaneRequestChannel))
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown() = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      dataPlaneRequestChannel.shutdown()
      controlPlaneRequestChannelOpt.foreach(_.shutdown())
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
      if (acceptor != null) {
        acceptor.serverChannel.socket.getLocalPort
      } else {
        controlPlaneAcceptorOpt.map (_.serverChannel.socket().getLocalPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, listenersAdded)
    startDataPlaneProcessors()
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      dataPlaneAcceptors.asScala.remove(endpoint).foreach(_.shutdown())
    }
  }

  def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
    connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
  }

  def updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides: Map[String, Int]): Unit = {
    info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
    connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
  }

  // `protected` for test usage
  protected[network] def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  private[network] def dataPlaneProcessor(index: Int): Processor = dataPlaneProcessors.get(index)

}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"
  val DataPlaneThreadPrefix = "data-plane"
  val ControlPlaneThreadPrefix = "control-plane"
  val DataPlaneMetricPrefix = ""
  val ControlPlaneMetricPrefix = "ControlPlane"
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress()}")
      connectionQuotas.dec(channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
// 连接器  接收连接
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              connectionQuotas: ConnectionQuotas,
                              metricPrefix: String) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  // nio selector 实例
  private val nioSelector = NSelector.open()
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  // 处理器 数组
  private val processors = new ArrayBuffer[Processor]()
  private val processorsStarted = new AtomicBoolean
  private val blockedPercentMeter = newMeter(s"${metricPrefix}AcceptorBlockedPercent",
    "blocked time", TimeUnit.NANOSECONDS, Map(ListenerMetricTag -> endPoint.listenerName.value))
  // 添加处理器到 acceptor中
  private[network] def addProcessors(newProcessors: Buffer[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors, processorThreadPrefix)
  }

  private[network] def startProcessors(processorThreadPrefix: String): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors, processorThreadPrefix)
    }
  }
  //  启动 processor
  private def startProcessors(processors: Seq[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(s"${processorThreadPrefix}-kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.shutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def shutdown(): Unit = {
    super.shutdown()
    synchronized {
      processors.foreach(_.shutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    // 注册 serverSocketChannel 到 nioselector中
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessorIndex = 0
      while (isRunning) {
        try {
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable) {
                  // 接收 新的连接
                  accept(key).foreach { socketChannel =>

                    // Assign the channel to the next processor (using round-robin) to which the
                    // channel can be added without blocking. If newConnections queue is full on
                    // all processors, block until the last one is able to accept a connection.
                    var retriesLeft = synchronized(processors.length)
                    var processor: Processor = null
                    do {
                      retriesLeft -= 1
                      processor = synchronized {
                        // adjust the index (if necessary) and retrieve the processor atomically for
                        // correct behaviour in case the number of processors is reduced dynamically
                        currentProcessorIndex = currentProcessorIndex % processors.length
                        // 从acceptor注册的processor中获取一个
                        processors(currentProcessorIndex)
                      }
                      currentProcessorIndex += 1
                      // assignNewConnection 分配新连接
                    } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
                  }
                } else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /**
  * Create a server socket to listen for connections on.
  */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    // 创建地址
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    // serverSocketChannel 实例创建
    val serverChannel = ServerSocketChannel.open()
    // 配置为 no block
    serverChannel.configureBlocking(false)
    // 接收buffer 设置
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      // 地址绑定
      serverChannel.socket.bind(socketAddress)
      info(s"Awaiting socket connections on s${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * Accept a new connection
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    // 接收操作
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      // 接收后, 并对接收到的 socket 进行一些 配置
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        close(socketChannel)
        None
    }
  }
  // 主要是调用 processor 来对连接进行进一步的处理
  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"

  val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               failedAuthenticationDelayMs: Int,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext,
                               connectionQueueSize: Int = ConnectionQueueSize) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }
  // 记录新连接
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  // inflight 等待响应
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  // 响应队列
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName,
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags)))
          .fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  val expiredConnectionsKilledCount = new Total()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", "socket-server-metrics", metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache,
      time))
  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    // 创建 kafka 封装的selector,其内部引用了 nio selector
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  override def run() {
    // 开始运行
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          // 把newConnections 中的连接 注册到nioselector上, 事件为SelectionKey.OP_READ
          configureNewConnections()
          // register any new responses for writing
          // 处理等待处理的response, 需要发送的响应则发送到对应kafkaChannel的 send字段,准备发送
          // closeReponse,则关闭对应的channel 等
          processNewResponses()
          // 对channel进行读写操作
          // 1. 如果有写操作,则把数据写出
          // 2. 如果有读操作,则把数据读出,并读取到 networkReceive,并记录下来
          poll()
          // 处理记录读取到的数据
          processCompletedReceives()
          // 处理那些已经发送完成的 sends
          processCompletedSends()
          // 对失联连接的处理
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private def processException(errorMessage: String, throwable: Throwable) {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable) {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }
  // 处理新的response,即把处理好的 response 传输出去
  private def processNewResponses() {
    var currentResponse: RequestChannel.Response = null
    // 从responseQueue队列中 获取 待处理的响应
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            // 把channel  unmute
            tryUnmuteChannel(channelId)

          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            // 如果是关闭的 response,则把对应的channel 进行关闭
            close(channelId)
          case _: StartThrottlingResponse =>
            // 开始限流
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // 结束 限流
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  // `protected` for test usage
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    // 获取到 connectionId
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {
      // 把responseSend 设置到 kafkaChannel上, 准备发送
      selector.send(responseSend)
      // 记录正在路上的响应
      inflightResponses += (connectionId -> response)
    }
  }

  private def nowNanosSupplier = new Supplier[java.lang.Long] {
    override def get(): java.lang.Long = time.nanoseconds()
  }
  //  对channel的处理,包括写数据 读数据
  private def poll() {
    val pollTimeout = if (newConnections.isEmpty) 300 else 0
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }
  // 处理 那些接收完成的 buffer
  private def processCompletedReceives() {
    // 遍历所有读取的数据
    selector.completedReceives.asScala.foreach { receive =>
      try {
        // 获取 对应的kafkaChannel
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            // 获取解析到的 请求头
            val header = RequestHeader.parse(receive.payload)
            if (header.apiKey() == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive, nowNanosSupplier))
              trace(s"Begin re-authentication: $channel")
            else {
              val nowNanos = time.nanoseconds()
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                // 关闭过期的 channel
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                val connectionId = receive.source
                // 请求上下文
                val context = new RequestContext(header, connectionId, channel.socketAddress,
                  channel.principal, listenerName, securityProtocol)
                // 解析请求
                val req = new RequestChannel.Request(processor = id, context = context,
                  startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics)
                // 记录下来此请求
                // 即把 事件放入到 一个queue中
                requestChannel.sendRequest(req)
                // 获取connectionId 对应的channel并mute
                selector.mute(connectionId)
                // 调用 connectionId 对应的kafkaChannel 进行事件的处理
                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
              }
            }
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
  }
  // 对于已经完成发送的 send 进行处理
  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      try {
        // 对于已经发送完成的 sends从 inflightResponses 中移除其 记录
        val response = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(response)

        // Invoke send completion callback
        // 调用对调方法
        response.onComplete.foreach(onComplete => onComplete(send))

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        handleChannelMuteEvent(send.destination, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
          s"Exception while processing completed send to ${send.destination}", e)
      }
    }
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  // 处理失去连接的channel
  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      try {
        // 获取失联的主机
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        // 移除connectionId对应的 请求
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
    // 存储一个新的连接到队列中
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
      // 这里放入队列使用了两种方式
      // 1. 第一个直接放入,不会阻塞,有可能失败
      // 2. 如果队列满了,则阻塞,直到队列空闲成功放入
    val accepted = {
      if (newConnections.offer(socketChannel))
        true
      else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }
    if (accepted)
      wakeup()
    accepted
  }

  /**
   * Register any new connections that have been queued up. The number of connections processed
   * in each iteration is limited to ensure that traffic and connection close notifications of
   * existing channels are handled promptly.
   */
    // 配置新连接
    // 1. 把newConnections中的新连接 封装到 kafkakChannel中
    // 2. 并把新连接注册到 nioselector中, 感兴趣事件为 SelectionKey.OP_READ
    // 3. 注册channel 到 idleExpireManager
  private def configureNewConnections() {
    var connectionsProcessed = 0
    // 把newConnections 队列中接收的socketChannel 注册到 nioselector中
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // 把channel 注册到 nioselector 中
        selector.register(connectionId(channel.socket), channel)
        connectionsProcessed += 1
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(channel.id)
    }
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }
  // 记录响应到 responseQueue, 并唤醒selector,使其立即进行一次 查询
  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    // 记录响应
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    // 从队列中获取 响应
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  // 找到 connectionId 对应的kafakaChannel,并对相应的事件进行处理
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)

  /**
   * Wakeup the thread for selection.
   */
    // 唤醒 nioselector
  override def wakeup() = selector.wakeup()

  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
    metrics.removeMetric(expiredConnectionsKilledCountMetricName)
  }

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  @volatile private var defaultMaxConnectionsPerIp = defaultMax
  @volatile private var maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")
