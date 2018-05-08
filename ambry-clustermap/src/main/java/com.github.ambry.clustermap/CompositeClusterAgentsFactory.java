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

import com.github.ambry.config.ClusterMapConfig;
import java.io.IOException;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory that creates a {@link CompositeClusterManager} and {@link ClusterParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
// TODO: 2018/3/19 by zmyer
public class CompositeClusterAgentsFactory implements ClusterAgentsFactory {
  //日志对象
  private static final Logger logger = LoggerFactory.getLogger(CompositeClusterAgentsFactory.class);
  //静态集群agent工厂对象
  private final StaticClusterAgentsFactory staticClusterAgentsFactory;
  //helix集群agent工厂对象
  private final HelixClusterAgentsFactory helixClusterAgentsFactory;
  //复合型集群管理器对象
  private CompositeClusterManager compositeClusterManager;
  //集群参与者对象
  private ClusterParticipant clusterParticipant;

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
   */
  // TODO: 2018/3/21 by zmyer
  public CompositeClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    //创建静态集群agent工厂对象
    staticClusterAgentsFactory =
        new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutFilePath, partitionLayoutFilePath);
    //创建helix集群agent工厂对象
    helixClusterAgentsFactory =
        new HelixClusterAgentsFactory(clusterMapConfig, staticClusterAgentsFactory.getMetricRegistry());
  }

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partitionLayout the {@link PartitionLayout} to use with the internal {@link StaticClusterManager}
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
   */
  // TODO: 2018/3/21 by zmyer
  CompositeClusterAgentsFactory(ClusterMapConfig clusterMapConfig, PartitionLayout partitionLayout)
      throws JSONException, IOException {
    staticClusterAgentsFactory = new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout);
    helixClusterAgentsFactory =
        new HelixClusterAgentsFactory(clusterMapConfig, staticClusterAgentsFactory.getMetricRegistry());
  }

  /**
   * Create and return a {@link CompositeClusterManager}.
   * @return the constructed {@link CompositeClusterManager}.
   * @throws Exception if constructing the underlying {@link StaticClusterManager} or the {@link HelixClusterManager}
   * throws an Exception.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public CompositeClusterManager getClusterMap() throws IOException {
    if (compositeClusterManager == null) {
      StaticClusterManager staticClusterManager = staticClusterAgentsFactory.getClusterMap();
      HelixClusterManager helixClusterManager = null;
      try {
        helixClusterManager = helixClusterAgentsFactory.getClusterMap();
      } catch (Exception e) {
        logger.error("Helix cluster manager instantiation failed with exception", e);
      }
      //创建复合型集群管理器
      compositeClusterManager = new CompositeClusterManager(staticClusterManager, helixClusterManager);
    }
    //返回结果
    return compositeClusterManager;
  }

  // TODO: 2018/3/19 by zmyer
  @Override
  public ClusterParticipant getClusterParticipant() throws IOException {
    if (clusterParticipant == null) {
      //创建集群参与者对象
      clusterParticipant = helixClusterAgentsFactory.getClusterParticipant();
    }
    //返回结果
    return clusterParticipant;
  }
}

