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
import com.github.ambry.server.AmbryHealthReport;
import com.github.ambry.utils.Utils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.healthcheck.HealthReportProvider;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link ClusterParticipant} that registers as a participant to a Helix cluster.
 */
// TODO: 2018/3/19 by zmyer
class HelixParticipant implements ClusterParticipant {
    //日志对象
    private final Logger logger = LoggerFactory.getLogger(getClass());
    //集群名称
    private final String clusterName;
    //zk连接信息
    private final String zkConnectStr;
    //helix工厂对象
    private final HelixFactory helixFactory;
    //helix管理器
    private HelixManager manager;
    //实例名称
    private String instanceName;

    /**
     * Instantiate a HelixParticipant.
     * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
     * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
     * @throws JSONException if there is an error in parsing the JSON serialized ZK connect string config.
     */
    // TODO: 2018/3/19 by zmyer
    HelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) throws IOException {
        clusterName = clusterMapConfig.clusterMapClusterName;
        this.helixFactory = helixFactory;
        if (clusterName.isEmpty()) {
            throw new IllegalStateException("Clustername is empty in clusterMapConfig");
        }
        try {
            zkConnectStr = ClusterMapUtils.parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
                    .get(clusterMapConfig.clusterMapDatacenterName)
                    .getZkConnectStr();
        } catch (JSONException e) {
            throw new IOException("Received JSON exception while parsing ZKInfo json string", e);
        }
    }

    /**
     * Initialize the participant by registering via the {@link HelixManager} as a participant to the associated Helix
     * cluster.
     * @param hostName the hostname to use when registering as a participant.
     * @param port the port to use when registering as a participant.
     * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
     * @throws IOException if there is an error connecting to the Helix cluster.
     */
    // TODO: 2018/3/20 by zmyer
    @Override
    public void initialize(String hostName, int port, List<AmbryHealthReport> ambryHealthReports) throws IOException {
        logger.info("Initializing participant");
        //获取实例名称
        instanceName = ClusterMapUtils.getInstanceName(hostName, port);
        //创建helix管理器
        manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
        //获取helix状态机器
        StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
        //为状态机注册状态模型
        stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory());
        //注册健康检查任务
        registerHealthReportTasks(stateMachineEngine, ambryHealthReports);
        try {
            //开始连接helix管理器
            manager.connect();
        } catch (Exception e) {
            throw new IOException("Exception while connecting to the Helix manager", e);
        }
        for (AmbryHealthReport ambryHealthReport : ambryHealthReports) {
            //为helix健康收集器注册健康检查
            manager.getHealthReportCollector().addHealthReportProvider((HealthReportProvider) ambryHealthReport);
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public synchronized boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
        if (!(replicaId instanceof AmbryReplica)) {
            throw new IllegalArgumentException(
                    "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
        }
        List<String> sealedReplicas = getSealedReplicas();
        if (sealedReplicas == null) {
            sealedReplicas = new ArrayList<>();
        }
        //读取分区id
        String partitionId = replicaId.getPartitionId().toPathString();
        boolean success = true;
        if (!isSealed && sealedReplicas.contains(partitionId)) {
            //将具体的分区id从列表中删除
            sealedReplicas.remove(partitionId);
            //设置sealed副本数据
            success = setSealedReplicas(sealedReplicas);
        } else if (isSealed && !sealedReplicas.contains(partitionId)) {
            //直接将分区id插入到集合中
            sealedReplicas.add(partitionId);
            //同时设置sealed属性
            success = setSealedReplicas(sealedReplicas);
        }
        return success;
    }

    /**
     * Disconnect from the {@link HelixManager}.
     */
    // TODO: 2018/3/22 by zmyer
    @Override
    public void close() {
        if (manager != null) {
            manager.disconnect();
            manager = null;
        }
    }

    /**
     * Register {@link HelixHealthReportAggregatorTask}s for appropriate {@link AmbryHealthReport}s.
     * @param engine the {@link StateMachineEngine} to register the task state model.
     * @param healthReports the {@link List} of {@link AmbryHealthReport}s that may require the registration of
     * corresponding {@link HelixHealthReportAggregatorTask}s.
     */
    // TODO: 2018/3/22 by zmyer
    private void registerHealthReportTasks(StateMachineEngine engine, List<AmbryHealthReport> healthReports) {
        Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
        for (final AmbryHealthReport healthReport : healthReports) {
            if (healthReport.getAggregateIntervalInMinutes() != Utils.Infinite_Time) {
                // register cluster wide aggregation task for the health report
                //为健康检查注册聚合任务
                taskFactoryMap.put(
                        String.format("%s_%s", HelixHealthReportAggregatorTask.TASK_COMMAND_PREFIX,
                                healthReport.getReportName()),
                        new TaskFactory() {
                            @Override
                            public Task createNewTask(TaskCallbackContext context) {
                                return new HelixHealthReportAggregatorTask(context,
                                        healthReport.getAggregateIntervalInMinutes(),
                                        healthReport.getReportName(), healthReport.getQuotaStatsFieldName());
                            }
                        });
            }
        }
        if (!taskFactoryMap.isEmpty()) {
            engine.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
                    new TaskStateModelFactory(manager, taskFactoryMap));
        }
    }

    /**
     * Get the list of sealed replicas from the HelixAdmin
     * @return list of sealed replicas from HelixAdmin
     */
    // TODO: 2018/3/22 by zmyer
    private List<String> getSealedReplicas() {
        //获取helix管理对象
        HelixAdmin helixAdmin = manager.getClusterManagmentTool();
        //获取指定实例配置信息
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
        if (instanceConfig == null) {
            throw new IllegalStateException(
                    "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName +
                            "\"");
        }
        //获取指定的副本信息
        return ClusterMapUtils.getSealedReplicas(instanceConfig);
    }

    /**
     * Set the list of sealed replicas in the HelixAdmin
     * @param sealedReplicas list of sealed replicas to be set in the HelixAdmin
     * @return whether the operation succeeded or not
     */
    // TODO: 2018/3/22 by zmyer
    private boolean setSealedReplicas(List<String> sealedReplicas) {
        HelixAdmin helixAdmin = manager.getClusterManagmentTool();
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
        if (instanceConfig == null) {
            throw new IllegalStateException(
                    "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName +
                            "\"");
        }
        //设置sealed副本数据
        instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedReplicas);
        //设置实例配置信息
        return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    }
}
