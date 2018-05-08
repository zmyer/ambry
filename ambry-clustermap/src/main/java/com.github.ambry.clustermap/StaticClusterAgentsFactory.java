/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.json.JSONException;
import org.json.JSONObject;

import static com.github.ambry.utils.Utils.*;


/**
 * A class used to create the {@link StaticClusterManager} and {@link ClusterParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
// TODO: 2018/3/21 by zmyer
public class StaticClusterAgentsFactory implements ClusterAgentsFactory {
  //分区布置对象
  private final PartitionLayout partitionLayout;
  //集群配置信息
  private final ClusterMapConfig clusterMapConfig;
  //metric注册对象
  private final MetricRegistry metricRegistry;
  //静态集群管理器
  private StaticClusterManager staticClusterManager;
  //集群参与者对象
  private ClusterParticipant clusterParticipant;

  /**
   * Instantiate an instance of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if a JSON error is encountered while parsing the layout files.
   * @throws IOException if an I/O error is encountered accessing or reading the layout files.
   */
  // TODO: 2018/3/21 by zmyer
  public StaticClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    this(clusterMapConfig, new PartitionLayout(
        new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutFilePath)), clusterMapConfig),
        new JSONObject(readStringFromFile(partitionLayoutFilePath))));
  }

  /**
   * Instantiate an instance of this factory.
   * @param partitionLayout the {@link PartitionLayout} to use.
   */
  // TODO: 2018/3/21 by zmyer
  StaticClusterAgentsFactory(ClusterMapConfig clusterMapConfig, PartitionLayout partitionLayout) {
    this.clusterMapConfig = Objects.requireNonNull(clusterMapConfig, "ClusterMapConfig cannot be null");
    this.partitionLayout = Objects.requireNonNull(partitionLayout, "PartitionLayout cannot be null");
    this.metricRegistry = new MetricRegistry();
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public StaticClusterManager getClusterMap() {
    if (staticClusterManager == null) {
      //创建静态集群管理器
      staticClusterManager =
          new StaticClusterManager(partitionLayout, clusterMapConfig.clusterMapDatacenterName, metricRegistry);
    }
    //返回结果
    return staticClusterManager;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public ClusterParticipant getClusterParticipant() throws IOException {
    if (clusterParticipant == null) {
      //如果集群参与者为空，则提供默认实现
      clusterParticipant = new ClusterParticipant() {
        @Override
        public void initialize(String hostname, int port, List<AmbryHealthReport> ambryHealthReports) {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
          return false;
        }
      };
    }
    //返回结果
    return clusterParticipant;
  }

  /**
   * @return the {@link MetricRegistry} used when creating the {@link StaticClusterManager}
   */
  // TODO: 2018/3/21 by zmyer
  MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }
}

