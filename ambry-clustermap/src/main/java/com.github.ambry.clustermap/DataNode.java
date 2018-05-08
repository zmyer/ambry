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
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
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

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An extension of {@link DataNodeId} to be used within the {@link StaticClusterManager}.
 *
 * DataNode is uniquely identified by its hostname and port. A DataNode is in a {@link Datacenter}. A DataNode has zero
 * or more {@link Disk}s.
 */
// TODO: 2018/3/20 by zmyer
class DataNode extends DataNodeId {
  //所属的数据中心
  private final Datacenter datacenter;
  //主机名
  private final String hostname;
  //端口
  private final int portNum;
  //端口集合
  private final Map<PortType, Port> ports;
  //磁盘集合
  private final ArrayList<Disk> disks;
  //容量大小
  private final long rawCapacityInBytes;
  //资源状态策略
  private final ResourceStatePolicy dataNodeStatePolicy;
  //机架号
  private final long rackId;
  //SSL可用的数据中心集合
  private final ArrayList<String> sslEnabledDataCenters;
  //集群配置
  private final ClusterMapConfig clusterMapConfig;

  //日志对象
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // TODO: 2018/3/21 by zmyer
  DataNode(Datacenter datacenter, JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("DataNode " + jsonObject.toString());
    }
    this.datacenter = datacenter;
    this.clusterMapConfig = clusterMapConfig;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");

    //读取主机名
    this.hostname =
        clusterMapConfig.clusterMapResolveHostnames ? getFullyQualifiedDomainName(jsonObject.getString("hostname"))
            : jsonObject.getString("hostname");
    //读取端口
    this.portNum = jsonObject.getInt("port");
    try {
      //设置资源状态策略
      ResourceStatePolicyFactory resourceStatePolicyFactory =
          Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this,
              HardwareState.valueOf(jsonObject.getString("hardwareState")), clusterMapConfig);
      this.dataNodeStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    } catch (Exception e) {
      logger.error("Error creating resource state policy when instantiating a datanode " + e);
      throw new IllegalStateException(
          "Error creating resource state policy when instantiating a datanode: " + hostname + ":" + portNum, e);
    }
    //创建磁盘列表
    JSONArray diskJSONArray = jsonObject.getJSONArray("disks");
    this.disks = new ArrayList<Disk>(diskJSONArray.length());
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.disks.add(new Disk(this, diskJSONArray.getJSONObject(i), clusterMapConfig));
    }
    //计算容量大小
    this.rawCapacityInBytes = calculateRawCapacityInBytes();
    //设置端口集合
    this.ports = new HashMap<PortType, Port>();
    //将端口信息插入到集合中
    this.ports.put(PortType.PLAINTEXT, new Port(portNum, PortType.PLAINTEXT));
    //解析端口信息
    populatePorts(jsonObject);

    if (jsonObject.has("rackId")) {
      //设置机架号
      this.rackId = jsonObject.getLong("rackId");
      if (this.rackId < 0) {
        throw new IllegalStateException("Invalid rackId : " + this.rackId + " is less than 0");
      }
    } else {
      this.rackId = UNKNOWN_RACK_ID;
    }
    //验证
    validate();
  }

  // TODO: 2018/3/21 by zmyer
  private void populatePorts(JSONObject jsonObject) throws JSONException {
    if (jsonObject.has("sslport")) {
      //设置端口
      int sslPortNum = jsonObject.getInt("sslport");
      this.ports.put(PortType.SSL, new Port(sslPortNum, PortType.SSL));
    }
  }

  @Override
  public String getHostname() {
    return hostname;
  }

  @Override
  public int getPort() {
    return portNum;
  }

  @Override
  public boolean hasSSLPort() {
    return ports.containsKey(PortType.SSL);
  }

  /**
   * Gets the DataNode's SSL port number.
   *
   * @return Port number upon which to establish an SSL encrypted connection with the DataNodeId.
   * @throws IllegalStateException Thrown if no SSL port exists.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public int getSSLPort() {
    if (hasSSLPort()) {
      return ports.get(PortType.SSL).getPort();
    } else {
      throw new IllegalStateException("No SSL port exists for the Data Node " + hostname + ":" + portNum);
    }
  }

  /**
   * Returns the {@link Port} of this node to connect to. A {@link Port} will be automatically selected based on if
   * there is a need of establishing an SSL connection.
   *
   * @return {@link Port} to which the caller can connect to.
   * @throws IllegalStateException Thrown if an SSL connection is needed but no SSL port exists.
   */
  // TODO: 2018/3/21 by zmyer
  @Override
  public Port getPortToConnectTo() {
    if (sslEnabledDataCenters.contains(datacenter.getName())) {
      if (ports.containsKey(PortType.SSL)) {
        return ports.get(PortType.SSL);
      } else {
        throw new IllegalStateException("No SSL Port exists for the data node " + hostname + ":" + portNum);
      }
    }
    return ports.get(PortType.PLAINTEXT);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public HardwareState getState() {
    return dataNodeStatePolicy.isDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }

  void onNodeTimeout() {
    dataNodeStatePolicy.onError();
  }

  void onNodeResponse() {
    dataNodeStatePolicy.onSuccess();
  }

  boolean isDown() {
    return dataNodeStatePolicy.isDown();
  }

  @Override
  public String getDatacenterName() {
    return getDatacenter().getName();
  }

  Datacenter getDatacenter() {
    return datacenter;
  }

  long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  // TODO: 2018/3/21 by zmyer
  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (Disk disk : disks) {
      //统计每个磁盘的容量，并计算总和
      capacityInBytes += disk.getRawCapacityInBytes();
    }
    //返回结果
    return capacityInBytes;
  }

  List<Disk> getDisks() {
    return disks;
  }

  @Override
  public long getRackId() {
    return rackId;
  }

  // TODO: 2018/3/21 by zmyer
  protected void validateDatacenter() {
    if (datacenter == null) {
      throw new IllegalStateException("Datacenter cannot be null.");
    }
  }

  // TODO: 2018/3/21 by zmyer
  private void validateHostname() {
    if (clusterMapConfig.clusterMapResolveHostnames) {
      String fqdn = getFullyQualifiedDomainName(hostname);
      if (!fqdn.equals(hostname)) {
        throw new IllegalStateException(
            "Hostname for DataNode (" + hostname + ") does not match its fully qualified domain name: " + fqdn + ".");
      }
    }
  }

  // TODO: 2018/3/21 by zmyer
  private void validatePorts() {
    Set<Integer> portNumbers = new HashSet<Integer>();
    for (PortType portType : ports.keySet()) {
      int portNo = ports.get(portType).getPort();
      if (portNumbers.contains(portNo)) {
        throw new IllegalStateException("Same port number " + portNo + " found for two port types");
      }
      if (portNo < MIN_PORT) {
        throw new IllegalStateException("Invalid " + portType + " port : " + portNo + " is less than " + MIN_PORT);
      } else if (portNo > MAX_PORT) {
        throw new IllegalStateException("Invalid " + portType + " port : " + portNo + " is greater than " + MAX_PORT);
      }
      portNumbers.add(portNo);
    }
  }

  // TODO: 2018/3/21 by zmyer
  private void validate() {
    logger.trace("begin validate.");
    validateDatacenter();
    validateHostname();
    validatePorts();
    for (Disk disk : disks) {
      disk.validate();
    }
    logger.trace("complete validate.");
  }

  // TODO: 2018/3/21 by zmyer
  JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject().put("hostname", hostname).put("port", portNum);
    addSSLPortToJson(jsonObject);
    if (rackId >= 0) {
      jsonObject.put("rackId", getRackId());
    }
    jsonObject.put("hardwareState",
        dataNodeStatePolicy.isHardDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE)
        .put("disks", new JSONArray());
    for (Disk disk : disks) {
      jsonObject.accumulate("disks", disk.toJSONObject());
    }
    return jsonObject;
  }

  // TODO: 2018/3/21 by zmyer
  private void addSSLPortToJson(JSONObject jsonObject) throws JSONException {
    for (PortType portType : ports.keySet()) {
      if (portType == PortType.SSL) {
        jsonObject.put("sslport", ports.get(portType).getPort());
      }
    }
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
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

    DataNode dataNode = (DataNode) o;

    if (portNum != dataNode.portNum) {
      return false;
    }
    return hostname.equals(dataNode.hostname);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + portNum;
    return result;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public int compareTo(DataNodeId o) {
    if (o == null) {
      throw new NullPointerException("input argument null");
    }

    DataNode other = (DataNode) o;
    int compare = (portNum < other.portNum) ? -1 : ((portNum == other.portNum) ? 0 : 1);
    if (compare == 0) {
      compare = hostname.compareTo(other.hostname);
    }
    return compare;
  }
}
