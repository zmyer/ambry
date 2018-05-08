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

package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * The storage manager that handles all the stores on this node. The stores on each disk are handled by a
 * {@link DiskManager}
 */
// TODO: 2018/3/19 by zmyer
public class StorageManager {
    //分区磁盘管理器集合
    private final Map<PartitionId, DiskManager> partitionToDiskManager = new HashMap<>();
    //磁盘管理器集合
    private final List<DiskManager> diskManagers = new ArrayList<>();
    //存储管理器metric对象
    private final StorageManagerMetrics metrics;
    //计时器
    private final Time time;
    //日志对象
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

    /**
     * Constructs a {@link StorageManager}
     * @param storeConfig the settings for store configuration.
     * @param diskManagerConfig the settings for disk manager configuration
     * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
     * @param registry the {@link MetricRegistry} used for store-related metrics.
     * @param replicas all the replicas on this disk.
     * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
     * @param recovery the {@link MessageStoreRecovery} instance to use.
     * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
     * @param time the {@link Time} instance to use.
     */
    // TODO: 2018/3/19 by zmyer
    public StorageManager(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
            ScheduledExecutorService scheduler, MetricRegistry registry, List<? extends ReplicaId> replicas,
            StoreKeyFactory keyFactory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
            WriteStatusDelegate writeStatusDelegate, Time time) throws StoreException {
        verifyConfigs(storeConfig, diskManagerConfig);
        metrics = new StorageManagerMetrics(registry);
        StoreMetrics storeMainMetrics = new StoreMetrics(registry);
        StoreMetrics storeUnderCompactionMetrics = new StoreMetrics("UnderCompaction", registry);
        this.time = time;
        Map<DiskId, List<ReplicaId>> diskToReplicaMap = new HashMap<>();
        for (ReplicaId replica : replicas) {
            //读取磁盘id
            DiskId disk = replica.getDiskId();
            //将磁盘与副本的关系插入到集合中
            diskToReplicaMap.computeIfAbsent(disk, key -> new ArrayList<>()).add(replica);
        }
        for (Map.Entry<DiskId, List<ReplicaId>> entry : diskToReplicaMap.entrySet()) {
            //磁盘id
            DiskId disk = entry.getKey();
            //副本集合
            List<ReplicaId> replicasForDisk = entry.getValue();
            //创建磁盘管理器对象
            DiskManager diskManager =
                    new DiskManager(disk, replicasForDisk, storeConfig, diskManagerConfig, scheduler, metrics,
                            storeMainMetrics,
                            storeUnderCompactionMetrics, keyFactory, recovery, hardDelete, writeStatusDelegate, time);
            //将磁盘管理器插入到集合中
            diskManagers.add(diskManager);
            for (ReplicaId replica : replicasForDisk) {
                //遍历每个分区副本信息，将对应的分区和磁盘管理器的关系插入到集合中
                partitionToDiskManager.put(replica.getPartitionId(), diskManager);
            }
        }
    }

    /**
     * Verify that the {@link StoreConfig} and {@link DiskManagerConfig} has valid settings.
     * @param storeConfig the {@link StoreConfig} to verify.
     * @param diskManagerConfig the {@link DiskManagerConfig} to verify
     * @throws StoreException if the {@link StoreConfig} or {@link DiskManagerConfig} is invalid.
     */
    // TODO: 2018/3/22 by zmyer
    private void verifyConfigs(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig) throws StoreException {
    /* NOTE: We must ensure that the store never performs hard deletes on the part of the log that is not yet flushed.
       We do this by making sure that the retention period for deleted messages (which determines the end point for hard
       deletes) is always greater than the log flush period. */
        if (storeConfig.storeDeletedMessageRetentionDays
                < TimeUnit.SECONDS.toDays(storeConfig.storeDataFlushIntervalSeconds) + 1) {
            throw new StoreException("Message retention days must be greater than the store flush interval period",
                    StoreErrorCodes.Initialization_Error);
        }
        if (diskManagerConfig.diskManagerReserveFileDirName.length() == 0) {
            throw new StoreException("Reserve file directory name is empty", StoreErrorCodes.Initialization_Error);
        }
    }

    /**
     * Start the {@link DiskManager}s for all disks on this node.
     * @throws InterruptedException
     */
    // TODO: 2018/3/19 by zmyer
    public void start() throws InterruptedException {
        long startTimeMs = time.milliseconds();
        try {
            logger.info("Starting storage manager");
            List<Thread> startupThreads = new ArrayList<>();
            for (final DiskManager diskManager : diskManagers) {
                //创建磁盘管理器线程
                Thread thread = Utils.newThread("disk-manager-startup-" + diskManager.getDisk(), () -> {
                    try {
                        //启动磁盘管理器
                        diskManager.start();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Disk manager startup thread interrupted for disk " + diskManager.getDisk(), e);
                    }
                }, false);
                //启动线程
                thread.start();
                //将创建的线程对象插入到集合中
                startupThreads.add(thread);
            }
            for (Thread startupThread : startupThreads) {
                startupThread.join();
            }
            //更新统计信息
            metrics.initializeCompactionThreadsTracker(this, diskManagers.size());
            logger.info("Starting storage manager complete");
        } finally {
            metrics.storageManagerStartTimeMs.update(time.milliseconds() - startTimeMs);
        }
    }

    /**
     * @param id the {@link PartitionId} to find the store for.
     * @return the {@link Store} corresponding to the given {@link PartitionId}, or {@code null} if no store was found for
     *         that partition, or that store was not started.
     */
    // TODO: 2018/3/22 by zmyer
    public Store getStore(PartitionId id) {
        //首先获取磁盘管理器
        DiskManager diskManager = partitionToDiskManager.get(id);
        //根据具体的分区id,查找指定的存储对象
        return diskManager != null ? diskManager.getStore(id) : null;
    }

    /**
     * Schedules the {@link PartitionId} {@code id} for compaction next.
     * @param id the {@link PartitionId} of the {@link Store} to compact.
     * @return {@code true} if the scheduling was successful. {@code false} if not.
     */
    // TODO: 2018/3/22 by zmyer
    public boolean scheduleNextForCompaction(PartitionId id) {
        //首先获取磁盘管理器
        DiskManager diskManager = partitionToDiskManager.get(id);
        //然后压缩对应的分区
        return diskManager != null && diskManager.scheduleNextForCompaction(id);
    }

    /**
     * Disable compaction on the {@link PartitionId} {@code id}.
     * @param id the {@link PartitionId} of the {@link Store} on which compaction is disabled.
     * @return {@code true} if disabling was successful. {@code false} if not.
     */
    // TODO: 2018/3/22 by zmyer
    public boolean disableCompactionForBlobStore(PartitionId id) {
        //读取磁盘管理器
        DiskManager diskManager = partitionToDiskManager.get(id);
        //开始解压缩对应的分区
        return diskManager != null && diskManager.disableCompactionForBlobStore(id);
    }

    /**
     * Shutdown the {@link DiskManager}s for the disks on this node.
     * @throws StoreException
     * @throws InterruptedException
     */
    // TODO: 2018/3/19 by zmyer
    public void shutdown() throws InterruptedException {
        long startTimeMs = time.milliseconds();
        try {
            logger.info("Shutting down storage manager");
            List<Thread> shutdownThreads = new ArrayList<>();
            for (final DiskManager diskManager : diskManagers) {
                //创建关闭线程
                Thread thread = Utils.newThread("disk-manager-shutdown-" + diskManager.getDisk(), () -> {
                    try {
                        //关闭磁盘管理器
                        diskManager.shutdown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Disk manager shutdown thread interrupted for disk " + diskManager.getDisk(), e);
                    }
                }, false);
                //启动关闭线程
                thread.start();
                //将关闭线程插入集合
                shutdownThreads.add(thread);
            }
            //等待线程结束
            for (Thread shutdownThread : shutdownThreads) {
                shutdownThread.join();
            }
            logger.info("Shutting down storage manager complete");
        } finally {
            metrics.storageManagerShutdownTimeMs.update(time.milliseconds() - startTimeMs);
        }
    }

    /**
     * @return the number of compaction threads running.
     */
    // TODO: 2018/3/22 by zmyer
    int getCompactionThreadCount() {
        int count = 0;
        for (DiskManager diskManager : diskManagers) {
            if (diskManager.isCompactionExecutorRunning()) {
                //统计目前还有多少磁盘管理区压缩线程在运行中
                count++;
            }
        }
        //返回结果
        return count;
    }
}
