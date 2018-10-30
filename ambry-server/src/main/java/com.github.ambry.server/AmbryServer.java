/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.server;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaStatusDelegate;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.BlockingChannelConnectionPool;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SocketServer;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.Transformer;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * Ambry server
 */
// TODO: 2018/3/19 by zmyer
public class AmbryServer {
  //屏障对象
  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  //网络服务器
  private NetworkServer networkServer = null;
  //请求对象集合
  private AmbryRequests requests = null;
  //请求处理池对象
  private RequestHandlerPool requestHandlerPool = null;
  //调度器
  private ScheduledExecutorService scheduler = null;
  //存储管理器
  private StorageManager storageManager = null;
  //状态管理器
  private StatsManager statsManager = null;
  //副本管理器
  private ReplicationManager replicationManager = null;
  //日志对象
  private Logger logger = LoggerFactory.getLogger(getClass());
  //校验属性
  private final VerifiableProperties properties;
  //集群agent工厂对象
  private final ClusterAgentsFactory clusterAgentsFactory;
  //集群对象
  private ClusterMap clusterMap;
  //集群参入对象
  private ClusterParticipant clusterParticipant;
  //统计注册对象
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  //连接池对象
  private ConnectionPool connectionPool = null;
  //公告系统
  private final NotificationSystem notificationSystem;
  private ServerMetrics metrics = null;
  //计时器
  private Time time;

  // TODO: 2018/3/20 by zmyer
  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory, Time time)
      throws IOException {
    this(properties, clusterAgentsFactory, new LoggingNotificationSystem(), time);
  }

  // TODO: 2018/3/20 by zmyer
  public AmbryServer(VerifiableProperties properties, ClusterAgentsFactory clusterAgentsFactory,
      NotificationSystem notificationSystem, Time time) {
    this.properties = properties;
    this.clusterAgentsFactory = clusterAgentsFactory;
    this.notificationSystem = notificationSystem;
    this.time = time;
  }

  // TODO: 2018/3/19 by zmyer
  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      //获取集群集合
      clusterMap = clusterAgentsFactory.getClusterMap();
      logger.info("Initialized clusterMap");
      //获取集群参与者对象
      clusterParticipant = clusterAgentsFactory.getClusterParticipant();
      logger.info("Setting up JMX.");
      long startTime = SystemTime.getInstance().milliseconds();
      registry = clusterMap.getMetricRegistry();
      this.metrics = new ServerMetrics(registry);
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      //创建网络配置对象
      NetworkConfig networkConfig = new NetworkConfig(properties);
      //创建存储配置对象
      StoreConfig storeConfig = new StoreConfig(properties);
      //创建磁盘管理对象
      DiskManagerConfig diskManagerConfig = new DiskManagerConfig(properties);
      //创建服务器配置对象
      ServerConfig serverConfig = new ServerConfig(properties);
      //创建副本配置对象
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      //创建连接池配置对象
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      //创建SSL配置
      SSLConfig sslConfig = new SSLConfig(properties);
      //创建集群配置对象
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
      //创建状态管理对象
      StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
      // verify the configs
      properties.verify();

      //创建服务调度线程池
      scheduler = Utils.newScheduler(serverConfig.serverSchedulerNumOfthreads, false);
      logger.info("check if node exist in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      //检查当前启动的服务器是否已经存在
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null) {
        throw new IllegalArgumentException("The node " + networkConfig.hostName + ":" + networkConfig.port
            + "is not present in the clustermap. Failing to start the datanode");
      }

      //创建键值存储工厂对象
      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      //创建FindToken工厂对象
      FindTokenFactory findTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      //创建存储管理器
      storageManager =
          new StorageManager(storeConfig, diskManagerConfig, scheduler, registry, clusterMap.getReplicaIds(nodeId),
              storeKeyFactory, new BlobStoreRecovery(), new BlobStoreHardDelete(),
              new ReplicaStatusDelegate(clusterParticipant), time);
      storageManager.start();

      //创建连接池对象
      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, clusterMapConfig, registry);
      //启动连接池
      connectionPool.start();

      StoreKeyConverterFactory storeKeyConverterFactory =
          Utils.getObj(serverConfig.serverStoreKeyConverterFactory, properties,
              registry);

      replicationManager =
          new ReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager, storeKeyFactory,
              clusterMap, scheduler, nodeId, connectionPool, registry, notificationSystem, storeKeyConverterFactory,
              serverConfig.serverMessageTransformer);
      replicationManager.start();

      //端口集合
      ArrayList<Port> ports = new ArrayList<Port>();
      ports.add(new Port(networkConfig.port, PortType.PLAINTEXT));
      if (nodeId.hasSSLPort()) {
        ports.add(new Port(nodeId.getSSLPort(), PortType.SSL));
      }

      //创建网络服务器
      networkServer = new SocketServer(networkConfig, sslConfig, registry, ports);
      //请求处理对象
      requests =
          new AmbryRequests(storageManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId, registry,
              findTokenFactory, notificationSystem, replicationManager, storeKeyFactory,
              serverConfig.serverEnableStoreDataPrefetch, storeKeyConverterFactory);
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
          networkServer.getRequestResponseChannel(), requests);
      //启动网络服务器
      networkServer.start();

      logger.info("Creating StatsManager to publish stats");
      statsManager = new StatsManager(storageManager, clusterMap.getReplicaIds(nodeId), registry, statsConfig, time);
      if (serverConfig.serverStatsPublishLocalEnabled) {
        //启动状态管理器
        statsManager.start();
      }

      //ambry健康检查
      List<AmbryHealthReport> ambryHealthReports = new ArrayList<>();
      if (serverConfig.serverStatsPublishHealthReportEnabled) {
        ambryHealthReports.add(
            new QuotaHealthReport(statsManager, serverConfig.serverQuotaStatsAggregateIntervalInMinutes));
      }

      clusterParticipant.participate(ambryHealthReports);

      logger.info("started");
      //统计启动花费时间
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      //更新处理时间
      metrics.serverStartTimeInMs.update(processingTime);
      logger.info("Server startup time in Ms " + processingTime);
    } catch (Exception e) {
      logger.error("Error during startup", e);
      throw new InstantiationException("failure during startup " + e);
    }
  }

  /**
   * This method is expected to be called in the exit path as long as the AmbryServer instance construction was
   * successful. This is expected to be called even if {@link #startup()} did not succeed.
   */
  // TODO: 2018/3/19 by zmyer
  public void shutdown() {
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      logger.info("shutdown started");
      if (clusterParticipant != null) {
        clusterParticipant.close();
      }
      if (scheduler != null) {
        shutDownExecutorService(scheduler, 5, TimeUnit.MINUTES);
      }
      if (statsManager != null) {
        statsManager.shutdown();
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (requestHandlerPool != null) {
        requestHandlerPool.shutdown();
      }
      if (replicationManager != null) {
        replicationManager.shutdown();
      }
      if (storageManager != null) {
        storageManager.shutdown();
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
      if (reporter != null) {
        reporter.stop();
      }
      if (notificationSystem != null) {
        try {
          notificationSystem.close();
        } catch (IOException e) {
          logger.error("Error while closing notification system.", e);
        }
      }
      if (clusterMap != null) {
        clusterMap.close();
      }
      logger.info("shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down server", e);
    } finally {
      shutdownLatch.countDown();
      long processingTime = SystemTime.getInstance().milliseconds() - startTime;
      metrics.serverShutdownTimeInMs.update(processingTime);
      logger.info("Server shutdown time in Ms " + processingTime);
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}
