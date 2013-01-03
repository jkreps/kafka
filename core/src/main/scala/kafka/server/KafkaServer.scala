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

package kafka.server

import kafka.network.SocketServer
import kafka.log.LogConfig
import kafka.log.CleanerConfig
import kafka.log.LogManager
import kafka.utils._
import java.util.concurrent._
import java.io.File
import atomic.AtomicBoolean
import org.I0Itec.zkclient.ZkClient
import kafka.controller.{ControllerStats, KafkaController}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = SystemTime) extends Logging {
  this.logIdent = "[Kafka Server " + config.brokerId + "], "
  private var isShuttingDown = new AtomicBoolean(false)
  private var shutdownLatch = new CountDownLatch(1)
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null
  var logManager: LogManager = null
  var kafkaZookeeper: KafkaZooKeeper = null
  var replicaManager: ReplicaManager = null
  var apis: KafkaApis = null
  var kafkaController: KafkaController = null
  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
  var zkClient: ZkClient = null

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    info("starting")
    isShuttingDown = new AtomicBoolean(false)
    shutdownLatch = new CountDownLatch(1)

    /* start scheduler */
    kafkaScheduler.startup()

    /* start log manager */
    logManager = createLogManager(config)
    logManager.startup()

    socketServer = new SocketServer(config.brokerId,
                                    config.port,
                                    config.numNetworkThreads,
                                    config.numQueuedRequests,
                                    config.maxSocketRequestSize)

    socketServer.startup

    /* start client */
    kafkaZookeeper = new KafkaZooKeeper(config)
    // starting relevant replicas and leader election for partitions assigned to this broker
    kafkaZookeeper.startup

    info("Connecting to ZK: " + config.zkConnect)

    replicaManager = new ReplicaManager(config, time, kafkaZookeeper.getZookeeperClient, kafkaScheduler, logManager)

    kafkaController = new KafkaController(config, kafkaZookeeper.getZookeeperClient)
    apis = new KafkaApis(socketServer.requestChannel, replicaManager, kafkaZookeeper.getZookeeperClient, config.brokerId)
    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
    Mx4jLoader.maybeLoad

    // start the replica manager
    replicaManager.startup()
    // start the controller
    kafkaController.startup()
    // register metrics beans
    registerStats()
    info("started")
  }

  /**
   *  Forces some dynamic jmx beans to be registered on server startup.
   */
  private def registerStats() {
    BrokerTopicStats.getBrokerAllTopicStats()
    ControllerStats.offlinePartitionRate
    ControllerStats.uncleanLeaderElectionRate
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    info("shutting down")
    val canShutdown = isShuttingDown.compareAndSet(false, true);
    if (canShutdown) {
      if(requestHandlerPool != null)
        Utils.swallow(requestHandlerPool.shutdown())
      Utils.swallow(kafkaScheduler.shutdown())
      if(apis != null)
        Utils.swallow(apis.close())
      if(kafkaZookeeper != null)
        Utils.swallow(kafkaZookeeper.shutdown())
      if(replicaManager != null)
        Utils.swallow(replicaManager.shutdown())
      if(socketServer != null)
        Utils.swallow(socketServer.shutdown())
      if(logManager != null)
        Utils.swallow(logManager.shutdown())

      if(kafkaController != null)
        Utils.swallow(kafkaController.shutdown())

      shutdownLatch.countDown()
      info("shut down completed")
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager
  
  private def createLogManager(config: KafkaConfig): LogManager = {
    val topics = config.logCleanupPolicyMap.keys ++ 
                 config.logFileSizeMap.keys ++ 
                 config.flushIntervalMap.keys ++ 
                 config.logRetentionHoursMap.keys ++ 
                 config.logRetentionSizeMap.keys ++ 
                 config.logRollHoursMap.keys
    val logConfigs = for(topic <- topics) yield 
      topic -> LogConfig(segmentSize = config.logFileSizeMap.getOrElse(topic, config.logFileSize), 
                         segmentMs = 60 * 60 * 1000 * config.logRollHoursMap.getOrElse(topic, config.logRollHours),
                         flushInterval = config.flushInterval,
                         flushMs = config.flushIntervalMap.getOrElse(topic, config.defaultFlushIntervalMs).toLong,
                         retentionSize = config.logRetentionSizeMap.getOrElse(topic, config.logRetentionSize),
                         retentionMs = 60 * 60 * 1000 * config.logRetentionHoursMap.getOrElse(topic, config.logRetentionHours),
                         maxMessageSize = config.maxMessageSize,
                         maxIndexSize = config.logIndexMaxSizeBytes,
                         indexInterval = config.logIndexIntervalBytes,
                         fileDeleteDelayMs = config.logDeleteDelayMs,
                         minCleanableRatio = config.logCleanerMinCleanRatio,
                         dedupe = config.logCleanupPolicyMap.getOrElse(topic, config.logCleanupPolicy).trim.toLowerCase == "dedupe")
    val defaultLogConfig = LogConfig(segmentSize = config.logFileSize, 
                                     segmentMs = 60 * 60 * 1000 * config.logRollHours,
                                     flushInterval = config.flushInterval,
                                     flushMs = config.defaultFlushIntervalMs.toLong,
                                     retentionSize = config.logRetentionSize,
                                     retentionMs = 60 * 60 * 1000 * config.logRetentionHours,
                                     maxMessageSize = config.maxMessageSize,
                                     maxIndexSize = config.logIndexMaxSizeBytes,
                                     indexInterval = config.logIndexIntervalBytes,
                                     fileDeleteDelayMs = config.logDeleteDelayMs,
                                     minCleanableRatio = config.logCleanerMinCleanRatio,
                                     dedupe = config.logCleanupPolicy.trim.toLowerCase == "dedupe")
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
                                      bufferSize = config.logCleanerBufferSize,
                                      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond) // TODO: Fix me
    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
                   configs = logConfigs.toMap,
                   defaultConfig = defaultLogConfig,
                   cleanerConfig = CleanerConfig(),
                   flushCheckMs = config.flushSchedulerThreadRate,
                   retentionCheckMs = config.logCleanupIntervalMinutes * 60 * 1000,
                   scheduler = kafkaScheduler,
                   time = time)
  }

}


