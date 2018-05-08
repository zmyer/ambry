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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * A cluster manager that is a wrapper over a {@link StaticClusterManager} instance and a {@link HelixClusterManager}
 * instance and uses the {@link StaticClusterManager} as the source-of-truth. It relays events to both and checks for
 * and reports inconsistencies in the views from the two underlying cluster managers.
 */
// TODO: 2018/3/19 by zmyer
class CompositeClusterManager implements ClusterMap {
  final StaticClusterManager staticClusterManager;
  final HelixClusterManager helixClusterManager;
  final HelixClusterManagerMetrics helixClusterManagerMetrics;

  /**
   * Construct a CompositeClusterManager instance.
   * @param staticClusterManager the {@link StaticClusterManager} instance to use as the source-of-truth.
   * @param helixClusterManager the {@link HelixClusterManager} instance to use for comparison of views.
   */
  // TODO: 2018/3/20 by zmyer
  CompositeClusterManager(StaticClusterManager staticClusterManager, HelixClusterManager helixClusterManager) {
    if (staticClusterManager.getLocalDatacenterId() != helixClusterManager.getLocalDatacenterId()) {
      throw new IllegalStateException(
          "Datacenter ID in the static cluster map [" + staticClusterManager.getLocalDatacenterId()
              + "] does not match the one in helix [" + helixClusterManager.getLocalDatacenterId() + "]");
    }
    this.staticClusterManager = staticClusterManager;
    this.helixClusterManager = helixClusterManager;
    this.helixClusterManagerMetrics =
        helixClusterManager != null ? helixClusterManager.helixClusterManagerMetrics : null;
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    DuplicatingInputStream duplicatingInputStream = new DuplicatingInputStream(stream);
    duplicatingInputStream.mark(0);
    //从数据流中读取分区id
    PartitionId partitionIdStatic = staticClusterManager.getPartitionIdFromStream(duplicatingInputStream);
    if (helixClusterManager != null) {
      //重置数据流
      duplicatingInputStream.reset();
      //读取分区id
      PartitionId partitionIdDynamic = helixClusterManager.getPartitionIdFromStream(duplicatingInputStream);
      if (!partitionIdStatic.toString().equals(partitionIdDynamic.toString())) {
        //如果两个数据流不一致，则递增失赔率次数
        helixClusterManagerMetrics.getPartitionIdFromStreamMismatchCount.inc();
      }
    }
    //返回分区id
    return partitionIdStatic;
  }

  /**
   * Get writable partition ids from both the underlying {@link StaticClusterManager} and the underlying
   * {@link HelixClusterManager}. Compare the two and if there is a mismatch, update a metric.
   * @return a list of writable partition ids from the underlying {@link StaticClusterManager}.
   */
  // TODO: 2018/3/20 by zmyer
  @Override
  public List<PartitionId> getWritablePartitionIds() {
    //先从静态的集群管理器中读取可写的分区列表
    List<PartitionId> staticWritablePartitionIds = staticClusterManager.getWritablePartitionIds();
    if (helixClusterManager != null) {
      //对比动态集群管理中可写的分区列表，如果不一致，则更新失陪次数
      if (!areEqual(staticWritablePartitionIds, helixClusterManager.getWritablePartitionIds())) {
        helixClusterManagerMetrics.getAllPartitionIdsMismatchCount.inc();
      }
    }
    //返回可写的分区列表
    return staticWritablePartitionIds;
  }

  /**
   * Get all partition ids from both the underlying {@link StaticClusterManager} and the underlying
   * {@link HelixClusterManager}. Compare the two and if there is a mismatch, update a metric.
   * @return a list of partition ids from the underlying {@link StaticClusterManager}.
   */
  // TODO: 2018/3/20 by zmyer
  @Override
  public List<PartitionId> getAllPartitionIds() {
    //获取集群中所有的分区id
    List<PartitionId> staticPartitionIds = staticClusterManager.getAllPartitionIds();
    if (helixClusterManager != null) {
      //读取所有的分区id
      if (!areEqual(staticPartitionIds, helixClusterManager.getAllPartitionIds())) {
        helixClusterManagerMetrics.getAllPartitionIdsMismatchCount.inc();
      }
    }
    //返回结果集
    return staticPartitionIds;
  }

  /**
   * Check for existence of the given datacenter from both the static and the helix based cluster managers and update
   * a metric if there is a mismatch.
   * @param datacenterName name of datacenter
   * @return true if the datacenter exists in the underlying {@link StaticClusterManager}; false otherwise.
   */
  // TODO: 2018/3/20 by zmyer
  @Override
  public boolean hasDatacenter(String datacenterName) {
    //判断指定的数据中心是否存在
    boolean staticHas = staticClusterManager.hasDatacenter(datacenterName);
    if (helixClusterManager != null) {
      //判断数据中心是否存在
      boolean helixHas = helixClusterManager.hasDatacenter(datacenterName);
      if (staticHas != helixHas) {
        helixClusterManagerMetrics.hasDatacenterMismatchCount.inc();
      }
    }
    //返回结果
    return staticHas;
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public byte getLocalDatacenterId() {
    return staticClusterManager.getLocalDatacenterId();
  }

  /**
   * Return the {@link DataNodeId} associated with the given hostname and port in the underlying
   * {@link StaticClusterManager}.
   * Also get the associated data node from the underlying {@link HelixClusterManager} and if the two returned
   * DataNodeId
   * objects do not refer to the same actual instance, update a metric.
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return the {@link DataNodeId} associated with the given hostname and port in the underlying
   * {@link StaticClusterManager}.
   */
  // TODO: 2018/3/19 by zmyer
  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    //根据主机名和端口，返回对应的数据节点id
    DataNodeId staticDataNode = staticClusterManager.getDataNodeId(hostname, port);
    if (helixClusterManager != null) {
      //查找数据节点id
      DataNodeId helixDataNode = helixClusterManager.getDataNodeId(hostname, port);
      //判断获取的节点数据是否一致
      if (!staticDataNode.toString().equals(helixDataNode.toString())) {
        helixClusterManagerMetrics.getDataNodeIdMismatchCount.inc();
      }
    }
    return staticDataNode;
  }

  /**
   * Get the list of {@link ReplicaId}s associated with the given {@link DataNodeId} in the underlying
   * {@link StaticClusterManager}.
   * Also get the associated list of {@link ReplicaId}s from the underlying {@link HelixClusterManager} and verify
   * equality, updating a metric if there is a mismatch.
   * @param dataNodeId the {@link DataNodeId} for which the replicas are to be listed.
   * @return the list of {@link ReplicaId}s as present in the underlying {@link StaticClusterManager} for the given
   * node.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    //获取数据节点上存储的所有的副本id
    List<ReplicaId> staticReplicaIds = staticClusterManager.getReplicaIds(dataNodeId);
    if (helixClusterManager != null) {
      Set<String> staticReplicaIdStrings = new HashSet<>();
      for (ReplicaId replicaId : staticReplicaIds) {
        staticReplicaIdStrings.add(replicaId.toString());
      }

      Set<String> helixReplicaIdStrings = new HashSet<>();
      //获取数据节点上所有的存储的副本id
      DataNodeId ambryDataNode = helixClusterManager.getDataNodeId(dataNodeId.getHostname(), dataNodeId.getPort());
      for (ReplicaId replicaId : helixClusterManager.getReplicaIds(ambryDataNode)) {
        helixReplicaIdStrings.add(replicaId.toString());
      }

      //比较两个地方获取到的副本id信息
      if (!staticReplicaIdStrings.equals(helixReplicaIdStrings)) {
        helixClusterManagerMetrics.getReplicaIdsMismatchCount.inc();
      }
    }
    return staticReplicaIds;
  }

  /**
   * The list of {@link DataNodeId}s present in the underlying {@link StaticClusterManager}
   * Also verify that the underlying {@link HelixClusterManager} has the same set of nodes, if not, update a metric.
   * @return the list of {@link DataNodeId}s present in the underlying {@link StaticClusterManager}
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public List<DataNodeId> getDataNodeIds() {
    //获取集群中所有的数据节点id
    List<DataNodeId> staticDataNodeIds = staticClusterManager.getDataNodeIds();
    if (helixClusterManager != null) {
      Set<String> staticDataNodeIdStrings = new HashSet<>();
      for (DataNodeId dataNodeId : staticDataNodeIds) {
        staticDataNodeIdStrings.add(dataNodeId.toString());
      }

      Set<String> helixDataNodeIdStrings = new HashSet<>();
      //获取集群中所有的数据节点id
      for (DataNodeId dataNodeId : helixClusterManager.getDataNodeIds()) {
        helixDataNodeIdStrings.add(dataNodeId.toString());
      }

      //进行比较
      if (!staticDataNodeIdStrings.equals(helixDataNodeIdStrings)) {
        helixClusterManagerMetrics.getDataNodeIdsMismatchCount.inc();
      }
    }
    //返回结果
    return staticDataNodeIds;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public MetricRegistry getMetricRegistry() {
    return staticClusterManager.getMetricRegistry();
  }

  /**
   * Relay the event to both the underlying {@link StaticClusterManager} and the underlying {@link HelixClusterManager}.
   * @param replicaId the {@link ReplicaId} for which this event has occurred.
   * @param event the {@link ReplicaEventType}.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    //处理接收到的副本事件
    staticClusterManager.onReplicaEvent(replicaId, event);
    if (helixClusterManager != null) {
      //首先获取具体的副本对象
      AmbryReplica ambryReplica =
          helixClusterManager.getReplicaForPartitionOnNode(replicaId.getDataNodeId().getHostname(),
              replicaId.getDataNodeId().getPort(), replicaId.getPartitionId().toString());
      //开始处理副本事件
      helixClusterManager.onReplicaEvent(ambryReplica, event);
    }
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public void close() {
    //关闭静态集群管理器
    staticClusterManager.close();
    if (helixClusterManager != null) {
      //关闭helix集群管理器
      helixClusterManager.close();
    }
  }

  /**
   * Check if two lists of partitions are equivalent
   * @param partitionListOne {@link List} of {@link PartitionId}s to compare
   * @param partitionListTwo {@link List} of {@link AmbryPartition}s to compare
   * @return {@code true} if both list are equal, {@code false} otherwise
   */
  // TODO: 2018/3/21 by zmyer
  private boolean areEqual(List<PartitionId> partitionListOne, List<AmbryPartition> partitionListTwo) {
    Set<String> partitionStringsOne = new HashSet<>();
    for (PartitionId partitionId : partitionListOne) {
      partitionStringsOne.add(partitionId.toString());
    }
    Set<String> partitionStringsTwo = new HashSet<>();
    for (PartitionId partitionId : partitionListTwo) {
      partitionStringsTwo.add(partitionId.toString());
    }
    return partitionStringsOne.equals(partitionStringsTwo);
  }
}

/**
 * A helper implementation of {@link FilterInputStream} via which the same data can be read from an underlying stream
 * a second time using the mark/reset flow. It works similar to {@link java.io.BufferedInputStream}, except
 * that it does not do any extra buffering - it only reads as many bytes from the underlying stream as is
 * required to return in the immediate call as part of which the underlying reads are done.
 */
// TODO: 2018/3/20 by zmyer
class DuplicatingInputStream extends FilterInputStream {
  private enum Mode {
    SAVE_READS, SERVE_DUPLICATES
  }

  private Mode mode = Mode.SAVE_READS;
  private final ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
  private ByteArrayInputStream readStream;

  /**
   * Construct an instance. The initial mode is "save-reads".
   * @param in the underlying {@link InputStream} to read from.
   */
  DuplicatingInputStream(InputStream in) {
    super(in);
  }

  /**
   * Bytes read until the next reset() call are buffered and served after the reset.
   * @param readLimit parameter is ignored.
   */
  @Override
  public void mark(int readLimit) {
    mode = Mode.SAVE_READS;
    writeStream.reset();
  }

  @Override
  public void reset() {
    mode = Mode.SERVE_DUPLICATES;
    readStream = new ByteArrayInputStream(writeStream.toByteArray());
  }

  @Override
  public int read() throws IOException {
    byte[] data = new byte[1];
    if (read(data, 0, 1) == -1) {
      return -1;
    } else {
      return data[0] & 0xFF;
    }
  }

  @Override
  public int read(byte[] data, int offset, int len) throws IOException {
    int actuallyRead;
    if (mode == Mode.SERVE_DUPLICATES) {
      actuallyRead = readStream.read(data, offset, len);
    } else {
      actuallyRead = in.read(data, offset, len);
      if (actuallyRead != -1) {
        writeStream.write(data, offset, actuallyRead);
      }
    }
    return actuallyRead;
  }
}
