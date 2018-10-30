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
package com.github.ambry.clustermap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PartitionLayout of {@link Partition}s and {@link Replica}s on an Ambry cluster (see {@link HardwareLayout}).
 */
// TODO: 2018/3/20 by zmyer
class PartitionLayout {
  //最小的分区id
  private static final long MinPartitionId = 0;
  //硬件布置层对象
  private final HardwareLayout hardwareLayout;
  //集群名称
  private final String clusterName;
  //版本信息
  private final long version;
  //分区集合
  private final Map<ByteBuffer, Partition> partitionMap;

  //最大分区id
  private long maxPartitionId;
  //已分配的容量大小
  private long allocatedRawCapacityInBytes;
  //已分配可用的容量大小
  private long allocatedUsableCapacityInBytes;
  private final String localDatacenterName;
  private final ClusterMapUtils.PartitionSelectionHelper partitionSelectionHelper;

  //日志对象
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a PartitionLayout
   * @param hardwareLayout the {@link HardwareLayout} to use.
   * @param jsonObject the {@link JSONObject} that represents the partition layout
   * @param localDatacenterName the name of the local datacenter. Can be {@code null}.
   * @throws JSONException
   */
  public PartitionLayout(HardwareLayout hardwareLayout, JSONObject jsonObject, String localDatacenterName)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("PartitionLayout " + hardwareLayout + ", " + jsonObject.toString());
    }
    this.hardwareLayout = hardwareLayout;
    this.localDatacenterName = localDatacenterName;
    this.clusterName = jsonObject.getString("clusterName");
    this.version = jsonObject.getLong("version");
    this.partitionMap = new HashMap<>();
    for (int i = 0; i < jsonObject.getJSONArray("partitions").length(); ++i) {
      addPartition(new Partition(this, jsonObject.getJSONArray("partitions").getJSONObject(i)));
    }

    //资源验证
    validate();
    partitionSelectionHelper = new ClusterMapUtils.PartitionSelectionHelper(partitionMap.values(), localDatacenterName);
  }

  /**
   * Constructor for initial PartitionLayout
   * @param hardwareLayout the {@link JSONObject} that represents the partition layout
   * @param localDatacenterName the name of the local datacenter. Can be {@code null}.
   */
  public PartitionLayout(HardwareLayout hardwareLayout, String localDatacenterName) {
    if (logger.isTraceEnabled()) {
      logger.trace("PartitionLayout " + hardwareLayout);
    }
    this.hardwareLayout = hardwareLayout;
    this.localDatacenterName = localDatacenterName;
    this.clusterName = hardwareLayout.getClusterName();
    this.version = 1;
    this.maxPartitionId = MinPartitionId;
    this.partitionMap = new HashMap<>();
    validate();
    partitionSelectionHelper = new ClusterMapUtils.PartitionSelectionHelper(partitionMap.values(), localDatacenterName);
  }

  public HardwareLayout getHardwareLayout() {
    return hardwareLayout;
  }

  public String getClusterName() {
    return clusterName;
  }

  public long getVersion() {
    return version;
  }

  public long getPartitionCount() {
    return partitionMap.size();
  }

  // TODO: 2018/3/21 by zmyer
  public long getPartitionInStateCount(PartitionState partitionState) {
    int count = 0;
    for (Partition partition : partitionMap.values()) {
      //统计指定分区状态下的分区数目
      if (partition.getPartitionState() == partitionState) {
        count++;
      }
    }
    return count;
  }

  public List<PartitionId> getPartitions(String partitionClass) {
    return partitionSelectionHelper.getPartitions(partitionClass);
  }

  public List<PartitionId> getWritablePartitions(String partitionClass) {
    return partitionSelectionHelper.getWritablePartitions(partitionClass);
  }

  public long getAllocatedRawCapacityInBytes() {
    return allocatedRawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateAllocatedRawCapacityInBytes() {
    long allocatedRawCapacityInBytes = 0;
    for (Partition partition : partitionMap.values()) {
      allocatedRawCapacityInBytes += partition.getAllocatedRawCapacityInBytes();
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedUsableCapacityInBytes() {
    return allocatedUsableCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateAllocatedUsableCapacityInBytes() {
    long allocatedUsableCapacityInBytes = 0;
    for (Partition partition : partitionMap.values()) {
      allocatedUsableCapacityInBytes += partition.getReplicaCapacityInBytes();
    }
    return allocatedUsableCapacityInBytes;
  }

  /**
   * Adds Partition to and validates Partition is unique. A duplicate Partition results in an exception.
   */
  // TODO: 2018/3/21 by zmyer
  private void addPartition(Partition partition) {
    //将分区插入到分区集合中
    if (partitionMap.put(ByteBuffer.wrap(partition.getBytes()), partition) != null) {
      throw new IllegalStateException("Duplicate Partition detected: " + partition.toString());
    }
    if (partition.getId() >= maxPartitionId) {
      //更新最大分区id
      maxPartitionId = partition.getId() + 1;
    }
  }

  private void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("ClusterName cannot be null.");
    }
    //如果分区所述的集群名称与硬件层不一致，则直接异常
    if (!hardwareLayout.getClusterName().equals(clusterName)) {
      throw new IllegalStateException(
          "PartitionLayout cluster name does not match that of HardwareLayout: " + clusterName + " != " + hardwareLayout
              .getClusterName());
    }
  }

  private void validatePartitionIds() {
    for (Partition partition : partitionMap.values()) {
      long partitionId = partition.getId();
      if (partitionId < MinPartitionId) {
        throw new IllegalStateException("Partition has invalid ID: Less than " + MinPartitionId);
      }
      if (partitionId >= maxPartitionId) {
        throw new IllegalStateException("Partition has invalid ID: Greater than or equal to " + maxPartitionId);
      }
    }
  }

  private void validateUniqueness() {
    // Validate uniqueness of each logical component. Partition uniqueness is validated by method addPartition.
    Set<Replica> replicaSet = new HashSet<Replica>();

    //验证分区副本唯一性
    for (Partition partition : partitionMap.values()) {
      for (Replica replica : partition.getReplicas()) {
        if (!replicaSet.add(replica)) {
          throw new IllegalStateException("Duplicate Replica detected: " + replica.toString());
        }
      }
    }
  }

  private void validate() {
    logger.trace("begin validate.");
    //验证集群名称
    validateClusterName();
    //验证分区id
    validatePartitionIds();
    //验证唯一性
    validateUniqueness();
    this.allocatedRawCapacityInBytes = calculateAllocatedRawCapacityInBytes();
    this.allocatedUsableCapacityInBytes = calculateAllocatedUsableCapacityInBytes();
    if (localDatacenterName != null && !localDatacenterName.isEmpty()
        && hardwareLayout.findDatacenter(localDatacenterName) == null) {
      throw new IllegalArgumentException("Clustermap has no datacenter named " + localDatacenterName);
    }
    logger.trace("complete validate.");
  }

  private long getNewPartitionId() {
    long currentPartitionId = maxPartitionId;
    //递增分区id
    maxPartitionId++;
    //返回结果
    return currentPartitionId;
  }

  // TODO: 2018/3/21 by zmyer
  // Creates a Partition and corresponding Replicas for each specified disk
  public Partition addNewPartition(List<Disk> disks, long replicaCapacityInBytes, String partitionClass) {
    if (disks == null || disks.size() == 0) {
      throw new IllegalArgumentException("Disks either null or of zero length.");
    }

    Partition partition =
        new Partition(getNewPartitionId(), partitionClass, PartitionState.READ_WRITE, replicaCapacityInBytes);
    for (Disk disk : disks) {
      partition.addReplica(new Replica(partition, disk, hardwareLayout.getClusterMapConfig()));
    }
    //将该分区插入到分区列表中
    addPartition(partition);
    //验证
    validate();
    partitionSelectionHelper.updatePartitions(partitionMap.values(), localDatacenterName);

    return partition;
  }

  // TODO: 2018/3/21 by zmyer
  // Adds replicas to the partition for each specified disk
  public void addNewReplicas(Partition partition, List<Disk> disks) {
    if (partition == null || disks == null || disks.size() == 0) {
      throw new IllegalArgumentException("Partition or disks is null or disks is of zero length");
    }
    for (Disk disk : disks) {
      partition.addReplica(new Replica(partition, disk, hardwareLayout.getClusterMapConfig()));
    }
    validate();
    partitionSelectionHelper.updatePartitions(partitionMap.values(), localDatacenterName);
  }

  /**
   * Gets Partition with specified byte-serialized ID.
   *
   * @param stream byte-serialized partition ID
   * @return requested Partition else null.
   */
  // TODO: 2018/3/21 by zmyer
  public Partition getPartition(InputStream stream) throws IOException {
    //读取分区信息
    byte[] partitionBytes = Partition.readPartitionBytesFromStream(stream);
    //根据分区信息，读取具体的分区
    return partitionMap.get(ByteBuffer.wrap(partitionBytes));
  }

  // TODO: 2018/3/21 by zmyer
  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject().put("clusterName", hardwareLayout.getClusterName())
        .put("version", version)
        .put("partitions", new JSONArray());
    for (Partition partition : partitionMap.values()) {
      jsonObject.accumulate("partitions", partition.toJSONObject());
    }
    return jsonObject;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public String toString() {
    try {
      return toJSONObject().toString(2);
    } catch (JSONException e) {
      logger.error("JSONException caught in toString: {}", e.getCause());
    }
    return null;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionLayout that = (PartitionLayout) o;

    if (!clusterName.equals(that.clusterName)) {
      return false;
    }
    return hardwareLayout.equals(that.hardwareLayout);
  }
}
