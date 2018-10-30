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
import com.github.ambry.utils.Utils;
import java.io.File;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link DiskId} to be used within the {@link StaticClusterManager}.
 *
 * A Disk stores {@link Replica}s. Each Disk is hosted on one specific {@link DataNode}. Each Disk is uniquely
 * identified by its DataNode and mount path (the path to this Disk's device on its DataNode).
 */
// TODO: 2018/3/20 by zmyer
class Disk implements DiskId {
  //所属的数据节点
  private final DataNode dataNode;
  //挂载点
  private final String mountPath;
  //资源状态策略
  private final ResourceStatePolicy diskStatePolicy;
  //磁盘容量
  private long capacityInBytes;

  //日子对象
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // TODO: 2018/3/21 by zmyer
  Disk(DataNode dataNode, JSONObject jsonObject, ClusterMapConfig clusterMapConfig) throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("Disk " + jsonObject.toString());
    }
    this.dataNode = dataNode;
    this.mountPath = jsonObject.getString("mountPath");
    try {
      //设置资源策略
      ResourceStatePolicyFactory resourceStatePolicyFactory =
          Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this,
              HardwareState.valueOf(jsonObject.getString("hardwareState")), clusterMapConfig);
      this.diskStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    } catch (Exception e) {
      logger.error("Error creating resource state policy when instantiating a disk " + e);
      throw new IllegalStateException("Error creating resource state policy when instantiating a disk: " + mountPath,
          e);
    }
    this.capacityInBytes = jsonObject.getLong("capacityInBytes");
    //资源验证
    validate();
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public HardwareState getState() {
    // A Disk is unavailable if its DataNode is unavailable.
    return dataNode.getState() == HardwareState.AVAILABLE && !diskStatePolicy.isDown() ? HardwareState.AVAILABLE
        : HardwareState.UNAVAILABLE;
  }

  // TODO: 2018/3/21 by zmyer
  boolean isDown() {
    return diskStatePolicy.isDown();
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public long getRawCapacityInBytes() {
    return capacityInBytes;
  }

  DataNode getDataNode() {
    return dataNode;
  }

  // TODO: 2018/3/21 by zmyer
  HardwareState getHardState() {
    return diskStatePolicy.isHardDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }

  // TODO: 2018/3/21 by zmyer
  protected void validateDataNode() {
    if (dataNode == null) {
      throw new IllegalStateException("DataNode cannot be null.");
    }
  }

  // TODO: 2018/3/21 by zmyer
  private void validateMountPath() {
    if (mountPath == null) {
      throw new IllegalStateException("Mount path cannot be null.");
    }
    if (mountPath.length() == 0) {
      throw new IllegalStateException("Mount path cannot be zero-length string.");
    }
    File mountPathFile = new File(mountPath);
    if (!mountPathFile.isAbsolute()) {
      throw new IllegalStateException("Mount path has to be an absolute path.");
    }
  }

  // TODO: 2018/3/21 by zmyer
  protected void validate() {
    logger.trace("begin validate.");
    validateDataNode();
    validateMountPath();
    ClusterMapUtils.validateDiskCapacity(capacityInBytes);
    logger.trace("complete validate.");
  }

  // TODO: 2018/3/21 by zmyer
  JSONObject toJSONObject() throws JSONException {
    return new JSONObject().put("mountPath", mountPath)
        .put("hardwareState", getHardState().name())
        .put("capacityInBytes", capacityInBytes);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public String toString() {
    String dataNodeStr = dataNode == null ? "" : dataNode.getHostname() + ":" + dataNode.getPort() + ":";
    return "Disk[" + dataNodeStr + getMountPath() + "]";
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

    Disk disk = (Disk) o;

    if (!dataNode.equals(disk.dataNode)) {
      return false;
    }
    return mountPath.equals(disk.mountPath);
  }

  // TODO: 2018/3/21 by zmyer
  @Override
  public int hashCode() {
    int result = dataNode.hashCode();
    result = 31 * result + mountPath.hashCode();
    return result;
  }

  // TODO: 2018/3/21 by zmyer
  void onDiskError() {
    diskStatePolicy.onError();
  }

  // TODO: 2018/3/21 by zmyer
  void onDiskOk() {
    diskStatePolicy.onSuccess();
  }
}
