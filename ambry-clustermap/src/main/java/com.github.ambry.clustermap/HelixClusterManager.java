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
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An implementation of {@link ClusterMap} that makes use of Helix to dynamically manage the cluster information.
 *
 * @see <a href="http://helix.apache.org">http://helix.apache.org</a>
 */
// TODO: 2018/3/20 by zmyer
class HelixClusterManager implements ClusterMap {
  //日志对象
  private final Logger logger = LoggerFactory.getLogger(getClass());
  //集群名称
  private final String clusterName;
  //metrics注册对象
  private final MetricRegistry metricRegistry;
  //集群配置
  private final ClusterMapConfig clusterMapConfig;
  //数据中心集合
  private final ConcurrentHashMap<String, DcInfo> dcToDcZkInfo = new ConcurrentHashMap<>();
  //分区集合
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition = new ConcurrentHashMap<>();
  //数据节点集合
  private final ConcurrentHashMap<String, AmbryDataNode> instanceNameToAmbryDataNode = new ConcurrentHashMap<>();
  //分区副本集合
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas =
      new ConcurrentHashMap<>();
  //节点副本集合
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryReplica>> ambryDataNodeToAmbryReplicas =
      new ConcurrentHashMap<>();
  //磁盘集合
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryDisk>> ambryDataNodeToAmbryDisks = new ConcurrentHashMap<>();
  //分区集合
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap = new ConcurrentHashMap<>();
  //集群总容量
  private long clusterWideRawCapacityBytes;
  //集群已分配的容量大小
  private long clusterWideAllocatedRawCapacityBytes;
  //集群可用容量大小
  private long clusterWideAllocatedUsableCapacityBytes;
  //集群回调函数,主要用于查询
  private final HelixClusterManagerCallback helixClusterManagerCallback;
  //本地数据中心id
  private final byte localDatacenterId;
  //初始化异常对象
  private final AtomicReference<Exception> initializationException = new AtomicReference<>();
  //
  private final AtomicLong sealedStateChangeCounter = new AtomicLong(0);
  //集群统计对象
  final HelixClusterManagerMetrics helixClusterManagerMetrics;

  /**
   * Instantiate a HelixClusterManager.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this manager.
   * @param instanceName the String representation of the instance associated with this manager.
   * @throws IOException if there is an error in parsing the clusterMapConfig or in connecting with the associated
   *                     remote Zookeeper services.
   */
  // TODO: 2018/3/21 by zmyer
  HelixClusterManager(ClusterMapConfig clusterMapConfig, String instanceName, HelixFactory helixFactory,
      MetricRegistry metricRegistry) throws IOException {
    this.clusterMapConfig = clusterMapConfig;
    this.metricRegistry = metricRegistry;
    clusterName = clusterMapConfig.clusterMapClusterName;
    helixClusterManagerCallback = new HelixClusterManagerCallback();
    helixClusterManagerMetrics = new HelixClusterManagerMetrics(metricRegistry, helixClusterManagerCallback);
    try {
      //解析数据中心集合
      final Map<String, DcZkInfo> dataCenterToZkAddress =
          parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings);
      //构造初始化屏障对象
      final CountDownLatch initializationAttemptComplete = new CountDownLatch(dataCenterToZkAddress.size());
      for (Map.Entry<String, DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
        //dc名
        String dcName = entry.getKey();
        //zk连接信息
        String zkConnectStr = entry.getValue().getZkConnectStr();
        //创建集群变更处理对象
        ClusterChangeHandler clusterChangeHandler = new ClusterChangeHandler(dcName);
        // Initialize from every datacenter in a separate thread to speed things up.
        //创建初始化线程
        Utils.newThread(new Runnable() {
          @Override
          public void run() {
            try {
              //创建helix管理器
              HelixManager manager =
                  helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
              logger.info("Connecting to Helix manager at {}", zkConnectStr);
              //开始连接管理器
              manager.connect();
              logger.info("Established connection to Helix manager at {}", zkConnectStr);
              //创建dc信息
              DcInfo dcInfo = new DcInfo(dcName, entry.getValue(), manager, clusterChangeHandler);
              //将dc插入到集合中
              dcToDcZkInfo.put(dcName, dcInfo);

              // The initial instance config change notification is required to populate the static cluster
              // information, and only after that is complete do we want the live instance change notification to
              // come in. We do not need to do anything extra to ensure this, however, since Helix provides the initial
              // notification for a change from within the same thread that adds the listener, in the context of the add
              // call. Therefore, when the call to add a listener returns, the initial notification will have been
              // received and handled.
              //helix管理器注册实例配置变更监听器
              manager.addInstanceConfigChangeListener(clusterChangeHandler);
              logger.info("Registered instance config change listeners for Helix manager at {}", zkConnectStr);
              // Now register listeners to get notified on live instance change in every datacenter.
              //helix管理器注册实例变更监听器
              manager.addLiveInstanceChangeListener(clusterChangeHandler);
              logger.info("Registered live instance change listeners for Helix manager at {}", zkConnectStr);
            } catch (Exception e) {
              initializationException.compareAndSet(null, e);
            } finally {
              //初始化完毕
              initializationAttemptComplete.countDown();
            }
          }
        }, false).start();
      }
      //等待初始化结束
      initializationAttemptComplete.await();
    } catch (Exception e) {
      initializationException.compareAndSet(null, e);
    }
    if (initializationException.get() == null) {
      initializeCapacityStats();
      helixClusterManagerMetrics.initializeInstantiationMetric(true);
      helixClusterManagerMetrics.initializeDatacenterMetrics();
      helixClusterManagerMetrics.initializeDataNodeMetrics();
      helixClusterManagerMetrics.initializeDiskMetrics();
      helixClusterManagerMetrics.initializePartitionMetrics();
      helixClusterManagerMetrics.initializeCapacityMetrics();
    } else {
      helixClusterManagerMetrics.initializeInstantiationMetric(false);
      close();
      throw new IOException("Encountered exception while parsing json, connecting to Helix or initializing",
          initializationException.get());
    }
    //获取本地数据中心id
    localDatacenterId = dcToDcZkInfo.get(clusterMapConfig.clusterMapDatacenterName).dcZkInfo.getDcId();
  }

  /**
   * Initialize capacity statistics.
   */
  // TODO: 2018/3/21 by zmyer
  private void initializeCapacityStats() {
    for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
      for (AmbryDisk disk : disks) {
        clusterWideRawCapacityBytes += disk.getRawCapacityInBytes();
      }
    }
    for (Set<AmbryReplica> partitionReplicas : ambryPartitionToAmbryReplicas.values()) {
      long replicaCapacity = partitionReplicas.iterator().next().getCapacityInBytes();
      clusterWideAllocatedRawCapacityBytes += replicaCapacity * partitionReplicas.size();
      clusterWideAllocatedUsableCapacityBytes += replicaCapacity;
    }
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dcToDcZkInfo.containsKey(datacenterName);
  }

  @Override
  public byte getLocalDatacenterId() {
    return localDatacenterId;
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public AmbryDataNode getDataNodeId(String hostname, int port) {
    return instanceNameToAmbryDataNode.get(getInstanceName(hostname, port));
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public List<AmbryReplica> getReplicaIds(DataNodeId dataNodeId) {
    //如果数据节点类型不对，则直接报错
    if (!(dataNodeId instanceof AmbryDataNode)) {
      throw new IllegalArgumentException("Incompatible type passed in");
    }
    AmbryDataNode datanode = (AmbryDataNode) dataNodeId;
    //从节点副本集合中查找指定节点下的副本信息
    return new ArrayList<>(ambryDataNodeToAmbryReplicas.get(datanode));
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public List<AmbryDataNode> getDataNodeIds() {
    return new ArrayList<>(instanceNameToAmbryDataNode.values());
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    AmbryReplica replica = (AmbryReplica) replicaId;
    switch (event) {
      case Node_Response:
        //处理节点应答消息
        replica.getDataNodeId().onNodeResponse();
        break;
      case Node_Timeout:
        //处理节点超时消息
        replica.getDataNodeId().onNodeTimeout();
        break;
      case Disk_Error:
        //处理磁盘错误
        replica.getDiskId().onDiskError();
        break;
      case Disk_Ok:
        //
        replica.getDiskId().onDiskOk();
        break;
      case Partition_ReadOnly:
        replica.getPartitionId().onPartitionReadOnly();
        break;
    }
  }

  // TODO: 2018/3/20 by zmyer
  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    //从数据流中读取分区id字节信息
    byte[] partitionBytes = AmbryPartition.readPartitionBytesFromStream(stream);
    //获取分区id
    AmbryPartition partition = partitionMap.get(ByteBuffer.wrap(partitionBytes));
    if (partition == null) {
      throw new IOException("Partition id from stream is null");
    }
    return partition;
  }

  /**
   * @return list of partition ids that are in {@link PartitionState#READ_WRITE}.
   */
  // TODO: 2018/3/20 by zmyer
  @Override
  public List<AmbryPartition> getWritablePartitionIds() {
    List<AmbryPartition> writablePartitions = new ArrayList<>();
    List<AmbryPartition> healthyWritablePartitions = new ArrayList<>();
    //依次遍历每个分区信息
    for (AmbryPartition partition : partitionNameToAmbryPartition.values()) {
      if (partition.getPartitionState() == PartitionState.READ_WRITE) {
        //如果当前的分区可读写，则插入到结果集中
        writablePartitions.add(partition);
        if (areAllReplicasForPartitionUp(partition)) {
          //如果所有的副本都是在线状态，则将该分区插入到健康列表中
          healthyWritablePartitions.add(partition);
        }
      }
    }
    //返回结果集
    return healthyWritablePartitions.isEmpty() ? writablePartitions : healthyWritablePartitions;
  }

  /**
   * @return list of all partition ids in the cluster
   */
  // TODO: 2018/3/20 by zmyer
  @Override
  public List<AmbryPartition> getAllPartitionIds() {
    return new ArrayList<>(partitionNameToAmbryPartition.values());
  }

  /**
   * Disconnect from the HelixManagers associated with each and every datacenter.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public void close() {
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      //断开helix管理器
      dcInfo.helixManager.disconnect();
    }
    dcToDcZkInfo.clear();
  }

  /**
   * Check whether all replicas of the given {@link AmbryPartition} are up.
   * @param partition the {@link AmbryPartition} to check.
   * @return true if all associated replicas are up; false otherwise.
   */
  // TODO: 2018/3/20 by zmyer
  private boolean areAllReplicasForPartitionUp(AmbryPartition partition) {
    for (AmbryReplica replica : ambryPartitionToAmbryReplicas.get(partition)) {
      //依次遍历每个副本信息，检查状态，如果跪掉，则直接返回
      if (replica.isDown()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the unique {@link AmbryReplica} for a {@link AmbryPartition} on a {@link AmbryDataNode}.
   * @param hostname the hostname associated with the {@link AmbryDataNode}.
   * @param port the port associated with the {@link AmbryDataNode}.
   * @param partitionString the partition id string associated with the {@link AmbryPartition}.
   * @return the {@link AmbryReplica} associated with the given parameters.
   */
  // TODO: 2018/3/21 by zmyer
  AmbryReplica getReplicaForPartitionOnNode(String hostname, int port, String partitionString) {
    for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(getDataNodeId(hostname, port))) {
      //根据给定的主机名和端口以及分区信息，查找对应的副本信息
      if (replica.getPartitionId().toString().equals(partitionString)) {
        return replica;
      }
    }
    return null;
  }

  /**
   * An instance of this object is used to register as listener for Helix related changes in each datacenter. This
   * class is also responsible for handling events received.
   */
  // TODO: 2018/3/21 by zmyer
  private class ClusterChangeHandler implements InstanceConfigChangeListener, LiveInstanceChangeListener {
    private final String dcName;
    final Set<String> allInstances = new HashSet<>();
    private final Object notificationLock = new Object();
    private final AtomicBoolean instanceConfigInitialized = new AtomicBoolean(false);
    private final AtomicBoolean liveStateInitialized = new AtomicBoolean(false);

    /**
     * Initialize a ClusterChangeHandler in the given datacenter.
     * @param dcName the datacenter associated with this ClusterChangeHandler.
     */
    ClusterChangeHandler(String dcName) {
      this.dcName = dcName;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
      logger.trace("InstanceConfig change triggered in {} with: {}", dcName, configs);
      synchronized (notificationLock) {
        if (!instanceConfigInitialized.get()) {
          logger.info("Received initial notification for instance config change from {}", dcName);
          try {
            initializeInstances(configs);
          } catch (Exception e) {
            initializationException.compareAndSet(null, e);
          }
          instanceConfigInitialized.set(true);
        } else {
          updateSealedStateOfReplicas(configs);
        }
        sealedStateChangeCounter.incrementAndGet();
        helixClusterManagerMetrics.instanceConfigChangeTriggerCount.inc();
      }
    }

    /**
     * Populate the initial data from the admin connection. Create nodes, disks, partitions and replicas for the entire
     * cluster.
     * @throws Exception if creation of {@link AmbryDataNode}s or {@link AmbryDisk}s throw an Exception.
     */
    private void initializeInstances(List<InstanceConfig> instanceConfigs) throws Exception {
      logger.info("Initializing cluster information from {}", dcName);
      for (InstanceConfig instanceConfig : instanceConfigs) {
        String instanceName = instanceConfig.getInstanceName();
        logger.info("Adding node {} and its disks and replicas", instanceName);
        AmbryDataNode datanode =
            new AmbryDataNode(getDcName(instanceConfig), clusterMapConfig, instanceConfig.getHostName(),
                Integer.valueOf(instanceConfig.getPort()), getRackId(instanceConfig), getSslPortStr(instanceConfig));
        initializeDisksAndReplicasOnNode(datanode, instanceConfig);
        instanceNameToAmbryDataNode.put(instanceName, datanode);
        allInstances.add(instanceName);
      }
      logger.info("Initialized cluster information from {}", dcName);
    }

    /**
     * Go over the given list of {@link InstanceConfig}s and update the sealed states of replicas.
     * @param instanceConfigs the list of {@link InstanceConfig}s containing the up-to-date information about the
     *                        sealed states of replicas.
     */
    private void updateSealedStateOfReplicas(List<InstanceConfig> instanceConfigs) {
      for (InstanceConfig instanceConfig : instanceConfigs) {
        AmbryDataNode node = instanceNameToAmbryDataNode.get(instanceConfig.getInstanceName());
        HashSet<String> sealedReplicas = new HashSet<>(getSealedReplicas(instanceConfig));
        for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(node)) {
          replica.setSealedState(sealedReplicas.contains(replica.getPartitionId().toPathString()));
        }
      }
    }

    /**
     * Triggered whenever there is a change in the list of live instances.
     * @param liveInstances the list of all live instances (not a change set) at the time of this call.
     * @param changeContext the {@link NotificationContext} associated.
     */
    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      logger.trace("Live instance change triggered from {} with: {}", dcName, liveInstances);
      updateInstanceLiveness(liveInstances);
      if (!liveStateInitialized.get()) {
        logger.info("Received initial notification for live instance change from {}", dcName);
        liveStateInitialized.set(true);
      }
      helixClusterManagerMetrics.liveInstanceChangeTriggerCount.inc();
    }

    /**
     * Update the liveness states of existing instances based on the input.
     * @param liveInstances the list of instances that are up.
     */
    private void updateInstanceLiveness(List<LiveInstance> liveInstances) {
      synchronized (notificationLock) {
        Set<String> liveInstancesSet = new HashSet<>();
        for (LiveInstance liveInstance : liveInstances) {
          liveInstancesSet.add(liveInstance.getInstanceName());
        }
        for (String instanceName : allInstances) {
          if (liveInstancesSet.contains(instanceName)) {
            instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.AVAILABLE);
          } else {
            instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.UNAVAILABLE);
          }
        }
      }
    }

    /**
     * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
     * that partition is being constructed.
     * @param datanode the {@link AmbryDataNode} that is being initialized.
     * @param instanceConfig the {@link InstanceConfig} associated with this datanode.
     * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
     */
    private void initializeDisksAndReplicasOnNode(AmbryDataNode datanode, InstanceConfig instanceConfig)
        throws Exception {
      ambryDataNodeToAmbryReplicas.put(datanode, new HashSet<AmbryReplica>());
      ambryDataNodeToAmbryDisks.put(datanode, new HashSet<AmbryDisk>());
      List<String> sealedReplicas = getSealedReplicas(instanceConfig);
      Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
      for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
        String mountPath = entry.getKey();
        Map<String, String> diskInfo = entry.getValue();
        long capacityBytes = Long.valueOf(diskInfo.get(DISK_CAPACITY_STR));
        HardwareState state =
            diskInfo.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE;
        String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);

        // Create disk
        AmbryDisk disk = new AmbryDisk(clusterMapConfig, datanode, mountPath, state, capacityBytes);
        ambryDataNodeToAmbryDisks.get(datanode).add(disk);

        if (!replicasStr.isEmpty()) {
          List<String> replicaInfoList = Arrays.asList(replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR));
          for (String replicaInfo : replicaInfoList) {
            String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
            // partition name and replica name are the same.
            String partitionName = info[0];
            long replicaCapacity = Long.valueOf(info[1]);
            AmbryPartition mappedPartition =
                new AmbryPartition(Long.valueOf(partitionName), helixClusterManagerCallback);
            // Ensure only one AmbryPartition entry goes in to the mapping based on the name.
            AmbryPartition existing = partitionNameToAmbryPartition.putIfAbsent(partitionName, mappedPartition);
            if (existing != null) {
              mappedPartition = existing;
            }
            // mappedPartition is now the final mapped AmbryPartition object for this partition.
            synchronized (mappedPartition) {
              if (!ambryPartitionToAmbryReplicas.containsKey(mappedPartition)) {
                ambryPartitionToAmbryReplicas.put(mappedPartition,
                    Collections.newSetFromMap(new ConcurrentHashMap<>()));
                partitionMap.put(ByteBuffer.wrap(mappedPartition.getBytes()), mappedPartition);
              }
            }
            ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, datanode, replicaCapacity);
            // Create replica associated with this node.
            AmbryReplica replica =
                new AmbryReplica(mappedPartition, disk, replicaCapacity, sealedReplicas.contains(partitionName));
            ambryPartitionToAmbryReplicas.get(mappedPartition).add(replica);
            ambryDataNodeToAmbryReplicas.get(datanode).add(replica);
          }
        }
      }
    }

    /**
     * Ensure that the given partition is absent on the given datanode. This is called as part of an inline validation
     * done to ensure that two replicas of the same partition do not exist on the same datanode.
     * @param partition the {@link AmbryPartition} to check.
     * @param datanode the {@link AmbryDataNode} on which to check.
     * @param expectedReplicaCapacity the capacity expected for the replicas of the partition.
     */
    private void ensurePartitionAbsenceOnNodeAndValidateCapacity(AmbryPartition partition, AmbryDataNode datanode,
        long expectedReplicaCapacity) {
      for (AmbryReplica replica : ambryPartitionToAmbryReplicas.get(partition)) {
        if (replica.getDataNodeId().equals(datanode)) {
          throw new IllegalStateException("Replica already exists on " + datanode + " for " + partition);
        } else if (replica.getCapacityInBytes() != expectedReplicaCapacity) {
          throw new IllegalStateException("Expected replica capacity " + expectedReplicaCapacity + " is different from "
              + "the capacity of an existing replica " + replica.getCapacityInBytes());
        }
      }
    }
  }

  /**
   * Class that stores all the information associated with a datacenter.
   */
  // TODO: 2018/3/20 by zmyer
  private static class DcInfo {
    final String dcName;
    final DcZkInfo dcZkInfo;
    final HelixManager helixManager;
    final ClusterChangeHandler clusterChangeHandler;

    /**
     * Construct a DcInfo object with the given parameters.
     * @param dcName the associated datacenter name.
     * @param dcZkInfo the {@link DcZkInfo} associated with the DC.
     * @param helixManager the associated {@link HelixManager} for this datacenter.
     * @param clusterChangeHandler the associated {@link ClusterChangeHandler} for this datacenter.
     */
    DcInfo(String dcName, DcZkInfo dcZkInfo, HelixManager helixManager, ClusterChangeHandler clusterChangeHandler) {
      this.dcName = dcName;
      this.dcZkInfo = dcZkInfo;
      this.helixManager = helixManager;
      this.clusterChangeHandler = clusterChangeHandler;
    }
  }

  /**
   * A callback class used to query information from the {@link HelixClusterManager}
   */
  // TODO: 2018/3/21 by zmyer
  class HelixClusterManagerCallback implements ClusterManagerCallback {
    /**
     * Get all replica ids associated with the given {@link AmbryPartition}
     * @param partition the {@link AmbryPartition} for which to get the list of replicas.
     * @return the list of {@link AmbryReplica}s associated with the given partition.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      return new ArrayList<>(ambryPartitionToAmbryReplicas.get(partition));
    }

    /**
     * Get the value counter representing the sealed state change for partitions.
     * @return the value of the counter representing the sealed state change for partitions.
     */
    @Override
    public long getSealedStateChangeCounter() {
      return sealedStateChangeCounter.get();
    }

    /**
     * @return the count of datacenters in this cluster.
     */
    long getDatacenterCount() {
      return dcToDcZkInfo.size();
    }

    /**
     * @return a collection of datanodes in this cluster.
     */
    Collection<AmbryDataNode> getDatanodes() {
      return new ArrayList<>(instanceNameToAmbryDataNode.values());
    }

    /**
     * @return the count of the datanodes in this cluster.
     */
    long getDatanodeCount() {
      return instanceNameToAmbryDataNode.values().size();
    }

    /**
     * @return the count of datanodes in this cluster that are down.
     */
    long getDownDatanodesCount() {
      long count = 0;
      for (AmbryDataNode datanode : instanceNameToAmbryDataNode.values()) {
        if (datanode.getState() == HardwareState.UNAVAILABLE) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return a collection of all the disks in this datacenter.
     */
    Collection<AmbryDisk> getDisks() {
      List<AmbryDisk> disksToReturn = new ArrayList<>();
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        disksToReturn.addAll(disks);
      }
      return disksToReturn;
    }

    /**
     * @return the count of disks in this cluster.
     */
    long getDiskCount() {
      long count = 0;
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        count += disks.size();
      }
      return count;
    }

    /**
     * @return the count of disks in this cluster that are down.
     */
    long getDownDisksCount() {
      long count = 0;
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        for (AmbryDisk disk : disks) {
          if (disk.getState() == HardwareState.UNAVAILABLE) {
            count++;
          }
        }
      }
      return count;
    }

    /**
     * @return a collection of partitions in this cluster.
     */
    Collection<AmbryPartition> getPartitions() {
      return new ArrayList<>(partitionMap.values());
    }

    /**
     * @return the count of partitions in this cluster.
     */
    long getPartitionCount() {
      return partitionMap.size();
    }

    /**
     * @return the count of partitions in this cluster that are in read-write state.
     */
    long getPartitionReadWriteCount() {
      long count = 0;
      for (AmbryPartition partition : partitionMap.values()) {
        if (partition.getPartitionState() == PartitionState.READ_WRITE) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return the count of partitions that are in sealed (read-only) state.
     */
    long getPartitionSealedCount() {
      long count = 0;
      for (AmbryPartition partition : partitionMap.values()) {
        if (partition.getPartitionState() == PartitionState.READ_ONLY) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return the cluster wide raw capacity in bytes.
     */
    long getRawCapacity() {
      return clusterWideRawCapacityBytes;
    }

    /**
     * @return the cluster wide allocated raw capacity in bytes.
     */
    long getAllocatedRawCapacity() {
      return clusterWideAllocatedRawCapacityBytes;
    }

    /**
     * @return the cluster wide allocated usable capacity in bytes.
     */
    long getAllocatedUsableCapacity() {
      return clusterWideAllocatedUsableCapacityBytes;
    }
  }
}

