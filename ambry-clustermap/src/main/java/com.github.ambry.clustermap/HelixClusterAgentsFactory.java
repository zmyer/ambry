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
import java.io.IOException;


/**
 * A factory class to construct {@link HelixClusterManager} and {@link HelixParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
// TODO: 2018/3/19 by zmyer
public class HelixClusterAgentsFactory implements ClusterAgentsFactory {
  //集群配置信息
  private final ClusterMapConfig clusterMapConfig;
  //实例名称
  private final String instanceName;
  //helix工厂对象
  private final HelixFactory helixFactory;
  private final MetricRegistry metricRegistry;
  //helix集群管理器
  private HelixClusterManager helixClusterManager;
  //helix参与者对象
  private HelixParticipant helixParticipant;

  /**
   * Construct an object of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this factory.
   * @param hardwareLayoutFilePath unused.
   * @param partitionLayoutFilePath unused.
   */
  // TODO: 2018/3/21 by zmyer
  public HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) {
    this(clusterMapConfig, new MetricRegistry());
  }

  // TODO: 2018/3/21 by zmyer
  HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, MetricRegistry metricRegistry) {
    this.clusterMapConfig = clusterMapConfig;
    this.instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    helixFactory = new HelixFactory();
    this.metricRegistry = metricRegistry;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public HelixClusterManager getClusterMap() throws IOException {
    if (helixClusterManager == null) {
      //创建helix集群管理器
      helixClusterManager = new HelixClusterManager(clusterMapConfig, instanceName, helixFactory, metricRegistry);
    }
    return helixClusterManager;
  }

  // TODO: 2018/3/19 by zmyer
  @Override
  public HelixParticipant getClusterParticipant() throws IOException {
    if (helixParticipant == null) {
      //创建helix参与者对象
      helixParticipant = new HelixParticipant(clusterMapConfig, helixFactory);
    }
    return helixParticipant;
  }
}

