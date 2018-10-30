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

import com.github.ambry.config.ClusterMapConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Ambry hardwareLayout consists of a set of {@link Datacenter}s.
 */
// TODO: 2018/3/20 by zmyer
class HardwareLayout {
  //集群名称
  private final String clusterName;
  //版本信息
  private final long version;
  //数据中心集合
  private final ArrayList<Datacenter> datacenters;
  private final Map<Byte, Datacenter> datacenterById;
  private final long rawCapacityInBytes;
  //数据节点数目
  private final long dataNodeCount;
  //磁盘数目
  private final long diskCount;
  //数据节点状态列表
  private final Map<HardwareState, Long> dataNodeInHardStateCount;
  //磁盘状态列表
  private final Map<HardwareState, Long> diskInHardStateCount;
  //集群配置信息
  private final ClusterMapConfig clusterMapConfig;

  //日志对象
  private Logger logger = LoggerFactory.getLogger(getClass());

  // TODO: 2018/3/21 by zmyer
  public HardwareLayout(JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("HardwareLayout " + jsonObject.toString());
    }
    //读取集群名称
    this.clusterName = jsonObject.getString("clusterName");
    //读取版本信息
    this.version = jsonObject.getLong("version");
    this.clusterMapConfig = clusterMapConfig;

    //读取数据中心集合
    this.datacenters = new ArrayList<Datacenter>(jsonObject.getJSONArray("datacenters").length());
    this.datacenterById = new HashMap<>();
    for (int i = 0; i < jsonObject.getJSONArray("datacenters").length(); ++i) {
      Datacenter datacenter =
          new Datacenter(this, jsonObject.getJSONArray("datacenters").getJSONObject(i), clusterMapConfig);
      this.datacenters.add(datacenter);
      datacenterById.put(datacenter.getId(), datacenter);
    }

    //计算总的容量大小
    this.rawCapacityInBytes = calculateRawCapacityInBytes();
    //计算数据节点数目
    this.dataNodeCount = calculateDataNodeCount();
    //计算磁盘数目
    this.diskCount = calculateDiskCount();
    //计算数据节点状态集合
    this.dataNodeInHardStateCount = calculateDataNodeInHardStateCount();
    //计算磁盘状态集合
    this.diskInHardStateCount = calculateDiskInHardStateCount();

    validate();
  }

  public String getClusterName() {
    return clusterName;
  }

  public long getVersion() {
    return version;
  }

  // TODO: 2018/3/21 by zmyer
  public List<Datacenter> getDatacenters() {
    return datacenters;
  }

  public long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (Datacenter datacenter : datacenters) {
      //统计每个数据中心的容量大小，计算总和
      capacityInBytes += datacenter.getRawCapacityInBytes();
    }
    return capacityInBytes;
  }

  public long getDatacenterCount() {
    return datacenters.size();
  }

  public long getDataNodeCount() {
    return dataNodeCount;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateDataNodeCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      //统计每个数据中心中包含的数据节点数目，计算总和
      count += datacenter.getDataNodes().size();
    }
    return count;
  }

  public long getDiskCount() {
    return diskCount;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateDiskCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        //统计每个数据中心下的数据节点的磁盘数目，计算总和
        count += dataNode.getDisks().size();
      }
    }
    return count;
  }

  // TODO: 2018/3/21 by zmyer
  public long getDataNodeInHardStateCount(HardwareState hardwareState) {
    //统计指定状态下的节点数目
    return dataNodeInHardStateCount.get(hardwareState);
  }

  ClusterMapConfig getClusterMapConfig() {
    return clusterMapConfig;
  }

  private Map<HardwareState, Long> calculateDataNodeInHardStateCount() {
    Map<HardwareState, Long> dataNodeInStateCount = new HashMap<HardwareState, Long>();
    for (HardwareState hardwareState : HardwareState.values()) {
      dataNodeInStateCount.put(hardwareState, new Long(0));
    }
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        //统计数据节点状态信息
        dataNodeInStateCount.put(dataNode.getState(), dataNodeInStateCount.get(dataNode.getState()) + 1);
      }
    }
    //返回结果
    return dataNodeInStateCount;
  }

  // TODO: 2018/3/21 by zmyer
  public long calculateUnavailableDataNodeCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNode.isDown()) {
          //统计不可用的数据节点数目
          count++;
        }
      }
    }
    return count;
  }

  // TODO: 2018/3/21 by zmyer
  public long getDiskInHardStateCount(HardwareState hardwareState) {
    return diskInHardStateCount.get(hardwareState);
  }

  // TODO: 2018/3/21 by zmyer
  private Map<HardwareState, Long> calculateDiskInHardStateCount() {
    Map<HardwareState, Long> diskInStateCount = new HashMap<HardwareState, Long>();
    for (HardwareState hardwareState : HardwareState.values()) {
      diskInStateCount.put(hardwareState, new Long(0));
    }
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          //统计磁盘状态信息
          diskInStateCount.put(disk.getState(), diskInStateCount.get(disk.getState()) + 1);
        }
      }
    }
    return diskInStateCount;
  }

  // TODO: 2018/3/21 by zmyer
  public long calculateUnavailableDiskCount() {
    long count = 0;
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (disk.isDown()) {
            //计算不可用的磁盘数目
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * Finds Datacenter by name
   *
   * @param datacenterName name of datacenter to be found
   * @return Datacenter or null if not found.
   */
  // TODO: 2018/3/21 by zmyer
  public Datacenter findDatacenter(String datacenterName) {
    for (Datacenter datacenter : datacenters) {
      //查找对应的数据中中心对象
      if (datacenter.getName().compareToIgnoreCase(datacenterName) == 0) {
        return datacenter;
      }
    }
    return null;
  }

  /**
   * Finds Datacenter by id
   *
   * @param id id of datacenter to find
   * @return Datacenter or null if not found.
   */
  public Datacenter findDatacenter(byte id) {
    return datacenterById.get(id);
  }

  /**
   * Finds DataNode by hostname and port. Note that hostname is converted to canonical hostname for comparison.
   *
   * @param hostname of datanode
   * @param port of datanode
   * @return DataNode or null if not found.
   */
  // TODO: 2018/3/20 by zmyer
  public DataNode findDataNode(String hostname, int port) {
    //获取主机名
    String canonicalHostname =
        clusterMapConfig.clusterMapResolveHostnames ? ClusterMapUtils.getFullyQualifiedDomainName(hostname) : hostname;
    logger.trace("host to find host {} port {}", canonicalHostname, port);
    for (Datacenter datacenter : datacenters) {
      logger.trace("datacenter {}", datacenter.getName());
      for (DataNode dataNode : datacenter.getDataNodes()) {
        //依次遍历每个数据节点，根据主机名和端口查找
        if (dataNode.getHostname().equals(canonicalHostname) && (dataNode.getPort() == port)) {
          return dataNode;
        }
      }
    }
    return null;
  }

  /**
   * Finds Disk by hostname, port, and mount path.
   *
   * @param hostname of datanode
   * @param port of datanode
   * @param mountPath of disk
   * @return Disk or null if not found.
   */
  // TODO: 2018/3/21 by zmyer
  public Disk findDisk(String hostname, int port, String mountPath) {
    //根据主机名和端口
    DataNode dataNode = findDataNode(hostname, port);
    if (dataNode != null) {
      for (Disk disk : dataNode.getDisks()) {
        if (disk.getMountPath().equals(mountPath)) {
          //根据挂载路径，查找具体的磁盘信息
          return disk;
        }
      }
    }
    return null;
  }

  // TODO: 2018/3/21 by zmyer
  protected void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be null.");
    } else if (clusterName.length() == 0) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be zero length.");
    }
  }

  // TODO: 2018/3/21 by zmyer
  // Validate each hardware component (Datacenter, DataNode, and Disk) are unique
  protected void validateUniqueness() throws IllegalStateException {
    logger.trace("begin validateUniqueness.");
    HashSet<Datacenter> datacenterSet = new HashSet<Datacenter>();
    HashSet<DataNode> dataNodeSet = new HashSet<DataNode>();
    HashSet<Disk> diskSet = new HashSet<Disk>();

    for (Datacenter datacenter : datacenters) {
      if (!datacenterSet.add(datacenter)) {
        throw new IllegalStateException("Duplicate Datacenter detected: " + datacenter.toString());
      }
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (!dataNodeSet.add(dataNode)) {
          throw new IllegalStateException("Duplicate DataNode detected: " + dataNode.toString());
        }
        for (Disk disk : dataNode.getDisks()) {
          if (!diskSet.add(disk)) {
            throw new IllegalStateException("Duplicate Disk detected: " + disk.toString());
          }
        }
      }
    }
    logger.trace("complete validateUniqueness.");
  }

  // TODO: 2018/3/21 by zmyer
  protected void validate() {
    logger.trace("begin validate.");
    validateClusterName();
    validateUniqueness();
    logger.trace("complete validate.");
  }

  // TODO: 2018/3/21 by zmyer
  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject =
        new JSONObject().put("clusterName", clusterName).put("version", version).put("datacenters", new JSONArray());
    for (Datacenter datacenter : datacenters) {
      jsonObject.accumulate("datacenters", datacenter.toJSONObject());
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

    HardwareLayout that = (HardwareLayout) o;

    if (!clusterName.equals(that.clusterName)) {
      return false;
    }
    return datacenters.equals(that.datacenters);
  }
}
