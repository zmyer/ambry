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

import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * StaticClusterManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
// TODO: 2018/3/20 by zmyer
class StaticClusterManager implements ClusterMap {
  //硬件布置层对象
  protected final HardwareLayout hardwareLayout;
  //分区布置层对象
  protected final PartitionLayout partitionLayout;
  //metric注册对象
  private final MetricRegistry metricRegistry;
  //机器metric对象
  private final ClusterMapMetrics clusterMapMetrics;
  //本地数据中心id
  private final byte localDatacenterId;

  //日志对象
  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * How many data nodes to put in a random sample for partition allocation. 2 node samples provided the best balance
   * of speed and allocation quality in testing. Larger samples (ie 3 nodes) took longer to generate but did not
   * improve the quality of the allocated partitions.
   */
  //每个分区副本数目
  private static final int NUM_CHOICES = 2;

  // TODO: 2018/3/21 by zmyer
  StaticClusterManager(PartitionLayout partitionLayout, String localDatacenterName, MetricRegistry metricRegistry) {
    if (logger.isTraceEnabled()) {
      logger.trace("StaticClusterManager " + partitionLayout);
    }
    //设置硬件层对象
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    //设置分区层对象
    this.partitionLayout = partitionLayout;
    this.metricRegistry = metricRegistry;
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
    localDatacenterId = localDatacenterName != null && !localDatacenterName.isEmpty() ? hardwareLayout.findDatacenter(
        localDatacenterName).getId() : ClusterMapUtils.UNKNOWN_DATACENTER_ID;
  }

  // TODO: 2018/3/21 by zmyer
  void persist(String hardwareLayoutPath, String partitionLayoutPath) throws IOException, JSONException {
    logger.trace("persist " + hardwareLayoutPath + ", " + partitionLayoutPath);
    //将硬件层对象序列化到文件
    writeJsonObjectToFile(hardwareLayout.toJSONObject(), hardwareLayoutPath);
    //将分区层对象序列化到文件
    writeJsonObjectToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
  }

  // Implementation of ClusterMap interface
  // --------------------------------------

  // TODO: 2018/3/20 by zmyer
  @Override
  public List<PartitionId> getWritablePartitionIds() {
    return partitionLayout.getWritablePartitions();
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public List<PartitionId> getAllPartitionIds() {
    return partitionLayout.getPartitions();
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    //读取分区id
    PartitionId partitionId = partitionLayout.getPartition(stream);
    if (partitionId == null) {
      throw new IOException("Partition id from stream is null");
    }
    return partitionId;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public boolean hasDatacenter(String datacenterName) {
    return hardwareLayout.findDatacenter(datacenterName) != null;
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public byte getLocalDatacenterId() {
    return localDatacenterId;
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    //获取所有的副本信息
    List<Replica> replicas = getReplicas(dataNodeId);
    //返回结果
    return new ArrayList<ReplicaId>(replicas);
  }

  // TODO: 2018/3/21 by zmyer
  List<Replica> getReplicas(DataNodeId dataNodeId) {
    List<Replica> replicas = new ArrayList<Replica>();
    for (PartitionId partition : partitionLayout.getPartitions()) {
      //依次遍历每个分区信息
      for (Replica replica : ((Partition) partition).getReplicas()) {
        //获取每个副本信息，比较存放的数据节点信息
        if (replica.getDataNodeId().equals(dataNodeId)) {
          //将查找成功的副本信息插入到结果集中
          replicas.add(replica);
        }
      }
    }
    //返回结果集
    return replicas;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> dataNodeIds = new ArrayList<DataNodeId>();
    //依次遍历每个数据中心，获取具体的数据节点信息
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      //获取每个数据中心包含的数据节点信息
      dataNodeIds.addAll(datacenter.getDataNodes());
    }
    //返回结果
    return dataNodeIds;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  // Administrative API
  // -----------------------

  long getRawCapacityInBytes() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  long getAllocatedRawCapacityInBytes() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  long getAllocatedUsableCapacityInBytes() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }

  // TODO: 2018/3/21 by zmyer
  long getAllocatedRawCapacityInBytes(Datacenter datacenter) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      //依次遍历每个分区对象
      for (Replica replica : ((Partition) partition).getReplicas()) {
        //获取每个分区对应的副本信息
        //获取副本对应的磁盘信息
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          //统计每个副本可以容纳空间大小
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    //返回结果
    return allocatedRawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  long getAllocatedRawCapacityInBytes(DataNodeId dataNode) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      //依次遍历每个分
      for (Replica replica : ((Partition) partition).getReplicas()) {
        //获取每个副本对应的磁盘信息
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          //统计当前数据节点的容量信息
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    //返回结果
    return allocatedRawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  long getAllocatedRawCapacityInBytes(Disk disk) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions()) {
      for (Replica replica : ((Partition) partition).getReplicas()) {
        Disk currentDisk = (Disk) replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  long getUnallocatedRawCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedRawCapacityInBytes();
  }

  // TODO: 2018/3/21 by zmyer
  long getUnallocatedRawCapacityInBytes(Datacenter datacenter) {
    return datacenter.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(datacenter);
  }

  // TODO: 2018/3/21 by zmyer
  long getUnallocatedRawCapacityInBytes(DataNode dataNode) {
    return dataNode.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(dataNode);
  }

  // TODO: 2018/3/21 by zmyer
  long getUnallocatedRawCapacityInBytes(Disk disk) {
    return disk.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(disk);
  }

  DataNode getDataNodeWithMostUnallocatedRawCapacity(Datacenter dc, Set nodesToExclude) {
    DataNode maxCapacityNode = null;
    List<DataNode> dataNodes = dc.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      if (!nodesToExclude.contains(dataNode) && (maxCapacityNode == null
          || getUnallocatedRawCapacityInBytes(dataNode) > getUnallocatedRawCapacityInBytes(maxCapacityNode))) {
        maxCapacityNode = dataNode;
      }
    }
    return maxCapacityNode;
  }

  // TODO: 2018/3/21 by zmyer
  Disk getDiskWithMostUnallocatedRawCapacity(DataNode node, long minCapacity) {
    Disk maxCapacityDisk = null;
    //获取节点上所有的磁盘对象
    List<Disk> disks = node.getDisks();
    for (Disk disk : disks) {
      if ((maxCapacityDisk == null || getUnallocatedRawCapacityInBytes(disk) > getUnallocatedRawCapacityInBytes(
          maxCapacityDisk)) && getUnallocatedRawCapacityInBytes(disk) >= minCapacity) {
        //查找容量最大的磁盘对象
        maxCapacityDisk = disk;
      }
    }
    //返回结果
    return maxCapacityDisk;
  }

  // TODO: 2018/3/21 by zmyer
  PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes);
  }

  // TODO: 2018/3/21 by zmyer
  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean checkEnoughUnallocatedRawCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      //如果存在空间不够的数据中心，则直接返回检查失败
      if (getUnallocatedRawCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient unallocated space in datacenter {} ({} bytes unallocated)", datacenter.getName(),
            getUnallocatedRawCapacityInBytes(datacenter));
        return false;
      }

      //每个数据中心包含的副本数量
      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          //如果当前磁盘有足够的空间的话，则需要递减分区副本数量
          if (getUnallocatedRawCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with unallocated space in datacenter {} for {} Replicas)", rcpd,
            datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }
    //检查成功
    return true;
  }

  /**
   * Get a sampling of {@code numChoices} random disks from a list of {@link DataNode}s and choose the
   * {@link Disk} on the {@link DataNode} with the most free space.
   * NOTE 1: This method will change the ordering of the nodes in {@code dataNodes}
   * NOTE 2: This method can return null, if a disk with enough free space could not be found.
   *
   * @param dataNodes the list of {@link DataNode}s to sample from
   * @param dataNodesUsed the set of {@link DataNode}s to exclude from the sample
   * @param replicaCapacityInBytes the minimum amount of free space that a disk in the sample should have
   * @param rackAware if {@code true}, only return disks in nodes that do not share racks with the nodes
   *                  in {@code dataNodesUsed}
   * @param numChoices how many disks in the sample to choose between
   * @return The {@link Disk} on the {@link DataNode} in the sample with the most free space, or {@code null } if a disk
   *         with enough free space could not be found.
   */
  // TODO: 2018/3/21 by zmyer
  private Disk getBestDiskCandidate(List<DataNode> dataNodes, Set<DataNode> dataNodesUsed, long replicaCapacityInBytes,
      boolean rackAware, int numChoices) {
    Set<Long> rackIdsUsed = new HashSet<>();
    if (rackAware) {
      for (DataNode dataNode : dataNodesUsed) {
        //首先需要统计已经在使用的数据节点所在的机架号
        rackIdsUsed.add(dataNode.getRackId());
      }
    }
    int numFound = 0;
    //数据节点数目
    int selectionBound = dataNodes.size();
    Random randomGen = new Random();
    Disk bestDisk = null;
    while ((selectionBound > 0) && (numFound < numChoices)) {
      //随机一个索引信息
      int selectionIndex = randomGen.nextInt(selectionBound);
      //根据索引信息获取一个数据节点对象
      DataNode candidate = dataNodes.get(selectionIndex);
      if (!dataNodesUsed.contains(candidate) && !rackIdsUsed.contains(candidate.getRackId())) {
        //如果当前的数据节点未被选中，并且相应的机架号也没有被选中，则查找该数据节点中最合适的磁盘对象
        Disk diskCandidate = getDiskWithMostUnallocatedRawCapacity(candidate, replicaCapacityInBytes);
        if (diskCandidate != null) {
          if ((bestDisk == null) || (getUnallocatedRawCapacityInBytes(diskCandidate.getDataNode())
              >= getUnallocatedRawCapacityInBytes(bestDisk.getDataNode()))) {
            //查找最合适的磁盘对象
            bestDisk = diskCandidate;
          }
          //递增查找次数
          numFound++;
        }
      }
      //递减选择次数
      selectionBound--;
      Collections.swap(dataNodes, selectionIndex, selectionBound);
    }
    //返回结果
    return bestDisk;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}.  Does not retry if fewer than
   * {@code replicaCountPerDatacenter} disks cannot be allocated.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param rackAware if {@code true}, attempt a rack-aware allocation
   * @return A list of {@link Disk}s
   */
  // TODO: 2018/3/21 by zmyer
  private List<Disk> getDiskCandidatesForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      Datacenter datacenter, boolean rackAware) {
    ArrayList<Disk> disksToAllocate = new ArrayList<Disk>();
    Set<DataNode> nodesToExclude = new HashSet<>();
    List<DataNode> dataNodes = new ArrayList<>(datacenter.getDataNodes());
    for (int i = 0; i < replicaCountPerDatacenter; i++) {
      //查找最合适的磁盘对象
      Disk bestDisk = getBestDiskCandidate(dataNodes, nodesToExclude, replicaCapacityInBytes, rackAware, NUM_CHOICES);
      if (bestDisk != null) {
        //将该磁盘对象插入待分配列表
        disksToAllocate.add(bestDisk);
        //将磁盘对应的数据节点插入到排除列表中
        nodesToExclude.add(bestDisk.getDataNode());
      } else {
        break;
      }
    }
    //返回待分配的磁盘列表
    return disksToAllocate;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}. Retry a non rack-aware allocation
   * in certain cases described below if {@code attemptNonRackAwareOnFailure} is enabled.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param attemptNonRackAwareOnFailure {@code true} if we should attempt a non rack-aware allocation if a rack-aware
   *                                     one is not possible.
   * @return a list of {@code replicaCountPerDatacenter} or fewer disks that can be allocated for a new partition in
   *         the specified datacenter
   */
  // TODO: 2018/3/21 by zmyer
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      Datacenter datacenter, boolean attemptNonRackAwareOnFailure) {
    List<Disk> disks;
    if (datacenter.isRackAware()) {
      //在数据中心分配指定数量的磁盘空间
      disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, true);
      if ((disks.size() < replicaCountPerDatacenter) && attemptNonRackAwareOnFailure) {
        System.err.println("Rack-aware allocation failed for a partition on datacenter:" + datacenter.getName()
            + "; attempting to perform a non rack-aware allocation.");
        //再重新分配一次
        disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
      }
    } else if (!attemptNonRackAwareOnFailure) {
      throw new IllegalArgumentException(
          "attemptNonRackAwareOnFailure is false, but the datacenter: " + datacenter.getName()
              + " does not have rack information");
    } else {
      //重新分配磁盘空间
      disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
    }

    //如果分配的磁盘数量小于所需要的，则直接记录错误日志
    if (disks.size() < replicaCountPerDatacenter) {
      System.err.println(
          "Could only allocate " + disks.size() + "/" + replicaCountPerDatacenter + " replicas in datacenter: "
              + datacenter.getName());
    }
    //返回结果
    return disks;
  }

  /**
   * Allocate partitions for {@code numPartitions} new partitions on all datacenters.
   *
   * @param numPartitions How many partitions to allocate.
   * @param replicaCountPerDatacenter The number of replicas per partition on each datacenter
   * @param replicaCapacityInBytes How large each replica (of a partition) should be
   * @param attemptNonRackAwareOnFailure {@code true} if we should attempt a non rack-aware allocation if a rack-aware
   *                                     one is not possible.
   * @return A list of the new {@link PartitionId}s.
   */
  // TODO: 2018/3/21 by zmyer
  List<PartitionId> allocatePartitions(int numPartitions, int replicaCountPerDatacenter, long replicaCapacityInBytes,
      boolean attemptNonRackAwareOnFailure) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);
    int partitionsAllocated = 0;
    //检查目前整个集群中是否有足够的空间来分配分区
    while (checkEnoughUnallocatedRawCapacity(replicaCountPerDatacenter, replicaCapacityInBytes)
        && partitionsAllocated < numPartitions) {
      List<Disk> disksToAllocate = new ArrayList<>();
      //遍历每个数据中心
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        //从当前数据中心中分配指定数量的磁盘空间
        List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter,
            attemptNonRackAwareOnFailure);
        //将待分配的磁盘空间插入到列表中
        disksToAllocate.addAll(disks);
      }
      //开始将新创建的分区信息插入到分区列表中
      partitions.add(partitionLayout.addNewPartition(disksToAllocate, replicaCapacityInBytes));
      //递增已经分配的分区数目
      partitionsAllocated++;
      System.out.println("Allocated " + partitionsAllocated + " new partitions so far.");
    }

    //返回结果
    return partitions;
  }

  /**
   * Add a set of replicas on a new datacenter for an existing partition.
   *
   * @param partitionId The partition to add to the new datacenter
   * @param dataCenterName The name of the new datacenter
   * @param attemptNonRackAwareOnFailure {@code true} if a non rack-aware allocation should be attempted if a rack-aware one
   *                            is not possible.
   */
  // TODO: 2018/3/21 by zmyer
  void addReplicas(PartitionId partitionId, String dataCenterName, boolean attemptNonRackAwareOnFailure) {
    //首先获取分区对应的所有的副本id
    List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
    Map<String, Integer> replicaCountByDatacenter = new HashMap<String, Integer>();
    long capacityOfReplicasInBytes = 0;
    // we ensure that the datacenter provided does not have any replicas
    for (ReplicaId replicaId : replicaIds) {
      //如果当前的副本信息存在于当前的数据中心，则抛出异常
      if (replicaId.getDataNodeId().getDatacenterName().compareToIgnoreCase(dataCenterName) == 0) {
        throw new IllegalArgumentException(
            "Data center " + dataCenterName + " provided already contains replica for partition " + partitionId);
      }
      //计算每个副本所需要占用的容量大小
      capacityOfReplicasInBytes = replicaId.getCapacityInBytes();
      //获取当前副本在数据中心中已经分配的副本数量
      Integer numberOfReplicas = replicaCountByDatacenter.get(replicaId.getDataNodeId().getDatacenterName());
      if (numberOfReplicas == null) {
        numberOfReplicas = new Integer(0);
      }
      //递增副本数量
      numberOfReplicas++;
      //将分配结果信息插入到集合中
      replicaCountByDatacenter.put(replicaId.getDataNodeId().getDatacenterName(), numberOfReplicas);
    }
    //如果结果为空，则抛出异常
    if (replicaCountByDatacenter.size() == 0) {
      throw new IllegalArgumentException("No existing replicas present for partition " + partitionId + " in cluster.");
    }
    // verify that all data centers have the same replica
    int numberOfReplicasPerDatacenter = 0;
    int index = 0;
    for (Map.Entry<String, Integer> entry : replicaCountByDatacenter.entrySet()) {
      if (index == 0) {
        //获取每个数据节点分配的副本数量
        numberOfReplicasPerDatacenter = entry.getValue();
      }
      //如果存在有数据节点上的分区数目不一致的情况，则直接异常
      if (numberOfReplicasPerDatacenter != entry.getValue()) {
        throw new IllegalStateException("Datacenters have different replicas for partition " + partitionId);
      }
      //索引递增
      index++;
    }
    //查找新数据中心
    Datacenter datacenterToAdd = hardwareLayout.findDatacenter(dataCenterName);
    //在新数据中心分配指定数目的副本
    List<Disk> disksForReplicas =
        allocateDisksForPartition(numberOfReplicasPerDatacenter, capacityOfReplicasInBytes, datacenterToAdd,
            attemptNonRackAwareOnFailure);
    //将分配结果插入分区层对象中
    partitionLayout.addNewReplicas((Partition) partitionId, disksForReplicas);
    System.out.println("Added partition " + partitionId + " to datacenter " + dataCenterName);
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

    StaticClusterManager that = (StaticClusterManager) o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null) {
      return false;
    }
    return !(partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    switch (event) {
      case Disk_Error:
        //处理磁盘错误
        ((Disk) replicaId.getDiskId()).onDiskError();
        break;
      case Disk_Ok:
        //处理磁盘写入成功
        ((Disk) replicaId.getDiskId()).onDiskOk();
        break;
      case Node_Timeout:
        //处理节点超时
        ((DataNode) replicaId.getDataNodeId()).onNodeTimeout();
        break;
      case Node_Response:
        //处理节点应答
        ((DataNode) replicaId.getDataNodeId()).onNodeResponse();
        break;
      case Partition_ReadOnly:
        //处理分区只读事件
        ((Partition) replicaId.getPartitionId()).onPartitionReadOnly();
        break;
    }
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public void close() {
    // No-op.
  }
}
