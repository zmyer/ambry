/*
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

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Manages all the stores on a disk.
 */
// TODO: 2018/3/19 by zmyer
class DiskManager {
    static final String CLEANUP_OPS_JOB_NAME = "cleanupOps";

    //分区对应的存储对象集合
    private final Map<PartitionId, BlobStore> stores = new HashMap<>();
    //磁盘id
    private final DiskId disk;
    //存储统计信息
    private final StorageManagerMetrics metrics;
    //计时器
    private final Time time;
    //磁盘IO调度器
    private final DiskIOScheduler diskIOScheduler;
    //长任务调度器
    private final ScheduledExecutorService longLivedTaskScheduler;
    //磁盘空间分配对象
    private final DiskSpaceAllocator diskSpaceAllocator;
    //压缩管理器
    private final CompactionManager compactionManager;
    //是否运行
    private boolean running = false;

    //日志对象
    private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);

    /**
     * Constructs a {@link DiskManager}
     * @param disk representation of the disk.
     * @param replicas all the replicas on this disk.
     * @param storeConfig the settings for store configuration.
     * @param diskManagerConfig the settings for disk manager configuration.
     * @param scheduler the {@link ScheduledExecutorService} for executing background tasks.
     * @param metrics the {@link StorageManagerMetrics} instance to use.
     * @param storeMainMetrics the {@link StoreMetrics} object used for store-related metrics.
     * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
     * @param keyFactory the {@link StoreKeyFactory} for parsing store keys.
     * @param recovery the {@link MessageStoreRecovery} instance to use.
     * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
     * @param time the {@link Time} instance to use.
     */
    // TODO: 2018/3/22 by zmyer
    DiskManager(DiskId disk, List<ReplicaId> replicas, StoreConfig storeConfig, DiskManagerConfig diskManagerConfig,
            ScheduledExecutorService scheduler, StorageManagerMetrics metrics, StoreMetrics storeMainMetrics,
            StoreMetrics storeUnderCompactionMetrics, StoreKeyFactory keyFactory, MessageStoreRecovery recovery,
            MessageStoreHardDelete hardDelete, WriteStatusDelegate writeStatusDelegate, Time time) {
        this.disk = disk;
        this.metrics = metrics;
        this.time = time;
        //创建磁盘IO调度器
        diskIOScheduler = new DiskIOScheduler(getThrottlers(storeConfig, time));
        //创建长任务调度器
        longLivedTaskScheduler = Utils.newScheduler(1, true);
        //创建磁盘空间分配器
        diskSpaceAllocator = new DiskSpaceAllocator(diskManagerConfig.diskManagerEnableSegmentPooling,
                new File(disk.getMountPath(), diskManagerConfig.diskManagerReserveFileDirName),
                diskManagerConfig.diskManagerRequiredSwapSegmentsPerSize, metrics);
        for (ReplicaId replica : replicas) {
            if (disk.equals(replica.getDiskId())) {
                //为所有在此磁盘上的分区副本，分配具体的存储对象
                BlobStore store =
                        new BlobStore(replica, storeConfig, scheduler, longLivedTaskScheduler, diskIOScheduler,
                                diskSpaceAllocator,
                                storeMainMetrics, storeUnderCompactionMetrics, keyFactory, recovery, hardDelete,
                                writeStatusDelegate,
                                time);
                //将具体的分配结果插入到集合中
                stores.put(replica.getPartitionId(), store);
            }
        }
        //创建压缩管理器
        compactionManager = new CompactionManager(disk.getMountPath(), storeConfig, stores.values(), metrics, time);
    }

    /**
     * Starts all the stores on this disk.
     * @throws InterruptedException
     */
    // TODO: 2018/3/22 by zmyer
    void start() throws InterruptedException {
        long startTimeMs = time.milliseconds();
        final AtomicInteger numStoreFailures = new AtomicInteger(0);
        try {
            //首先检查挂载点是否可以访问
            checkMountPathAccessible();

            List<Thread> startupThreads = new ArrayList<>();
            for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
                //为每个分区对于的储存对象分配线程，并启动
                Thread thread = Utils.newThread("store-startup-" + partitionAndStore.getKey(), () -> {
                    try {
                        //启动存储对象
                        partitionAndStore.getValue().start();
                    } catch (Exception e) {
                        //统计失败线程数目
                        numStoreFailures.incrementAndGet();
                        logger.error("Exception while starting store for the partition" + partitionAndStore.getKey(),
                                e);
                    }
                }, false);
                thread.start();
                //将启动线程插入到启动线程集合中
                startupThreads.add(thread);
            }
            for (Thread startupThread : startupThreads) {
                //等待线程结束
                startupThread.join();
            }

            //如果存在启动失败的情况，则记录错误日志
            if (numStoreFailures.get() > 0) {
                logger.error(
                        "Could not start " + numStoreFailures.get() + " out of " + stores.size() +
                                " stores on the disk " + disk);
            }

            // DiskSpaceAllocator startup. This happens after BlobStore startup because it needs disk space requirements
            // from each store.
            //开始启动磁盘空间分配器
            List<DiskSpaceRequirements> requirementsList = new ArrayList<>();
            for (BlobStore blobStore : stores.values()) {
                if (blobStore.isStarted()) {
                    //获取存储对象需要的磁盘空间大小
                    DiskSpaceRequirements requirements = blobStore.getDiskSpaceRequirements();
                    if (requirements != null) {
                        //将具体的磁盘空间需求对象插入到集合中
                        requirementsList.add(requirements);
                    }
                }
            }
            //初始化磁盘空间分配器
            diskSpaceAllocator.initializePool(requirementsList);

            //设置分区可压缩
            compactionManager.enable();

            //标记磁盘管理器运行状态
            running = true;
        } catch (StoreException e) {
            logger.error("Error while starting the DiskManager for " + disk.getMountPath()
                    + " ; no stores will be accessible on this disk.", e);
        } finally {
            if (!running) {
                metrics.totalStoreStartFailures.inc(stores.size());
                metrics.diskDownCount.inc();
            } else {
                metrics.totalStoreStartFailures.inc(numStoreFailures.get());
            }
            //统计磁盘启动耗时
            metrics.diskStartTimeMs.update(time.milliseconds() - startTimeMs);
        }
    }

    /**
     * Shuts down all the stores on this disk.
     * @throws InterruptedException
     */
    // TODO: 2018/3/22 by zmyer
    void shutdown() throws InterruptedException {
        long startTimeMs = time.milliseconds();
        try {
            //设置关闭标记
            running = false;
            //关闭磁盘压缩
            compactionManager.disable();
            //关闭磁盘IO调度器
            diskIOScheduler.disable();
            final AtomicInteger numFailures = new AtomicInteger(0);
            List<Thread> shutdownThreads = new ArrayList<>();
            for (final Map.Entry<PartitionId, BlobStore> partitionAndStore : stores.entrySet()) {
                //为每个存储对象创建关闭线程
                Thread thread = Utils.newThread("store-shutdown-" + partitionAndStore.getKey(), () -> {
                    try {
                        //关闭粗存储对象
                        partitionAndStore.getValue().shutdown();
                    } catch (Exception e) {
                        numFailures.incrementAndGet();
                        metrics.totalStoreShutdownFailures.inc();
                        logger.error("Exception while shutting down store {} on disk {}", partitionAndStore.getKey(),
                                disk, e);
                    }
                }, false);
                thread.start();
                shutdownThreads.add(thread);
            }
            for (Thread shutdownThread : shutdownThreads) {
                //等待关闭线程结束
                shutdownThread.join();
            }

            //如果存在关闭失败的情况，需要记录错误日志
            if (numFailures.get() > 0) {
                logger.error(
                        "Could not shutdown " + numFailures.get() + " out of " + stores.size() +
                                " stores on the disk " + disk);
            }
            //等待压缩管理器关闭
            compactionManager.awaitTermination();
            //关闭长任务调度器
            longLivedTaskScheduler.shutdown();
            //等待长任务调度器关闭
            if (!longLivedTaskScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.error("Could not terminate long live tasks after DiskManager shutdown");
            }
        } finally {
            //更新磁盘关闭耗时
            metrics.diskShutdownTimeMs.update(time.milliseconds() - startTimeMs);
        }
    }

    /**
     * @param id the {@link PartitionId} to find the store for.
     * @return the associated {@link Store}, or {@code null} if the partition is not on this disk, or the store is not
     *         started.
     */
    // TODO: 2018/3/22 by zmyer
    Store getStore(PartitionId id) {
        //获取存储对象
        BlobStore store = stores.get(id);
        //返回结果
        return (running && store != null && store.isStarted()) ? store : null;
    }

    /**
     * @return {@code true} if the compaction thread is running. {@code false} otherwise.
     */
    // TODO: 2018/3/22 by zmyer
    boolean isCompactionExecutorRunning() {
        return compactionManager.isCompactionExecutorRunning();
    }

    /**
     * @return the {@link DiskId} that is managed by this {@link DiskManager}.
     */
    // TODO: 2018/3/22 by zmyer
    DiskId getDisk() {
        return disk;
    }

    /**
     * Schedules the {@link PartitionId} {@code id} for compaction next.
     * @param id the {@link PartitionId} of the {@link BlobStore} to compact.
     * @return {@code true} if the scheduling was successful. {@code false} if not.
     */
    // TODO: 2018/3/22 by zmyer
    boolean scheduleNextForCompaction(PartitionId id) {
        BlobStore store = (BlobStore) getStore(id);
        return store != null && compactionManager.scheduleNextForCompaction(store);
    }

    /**
     * Disable compaction on the {@link PartitionId} {@code id}.
     * @param id the {@link PartitionId} of the {@link BlobStore} on which compaction is disabled.
     * @return {@code true} if disabling was successful. {@code false} if not.
     */
    // TODO: 2018/3/22 by zmyer
    boolean disableCompactionForBlobStore(PartitionId id) {
        BlobStore store = stores.get(id);
        return store != null && compactionManager.disableCompactionForBlobStore(store);
    }

    /**
     * Gets all the throttlers that the {@link DiskIOScheduler} will be constructed with.
     * @param config the {@link StoreConfig} with configuration values.
     * @param time the {@link Time} instance to use in the throttlers
     * @return the throttlers that the {@link DiskIOScheduler} will be constructed with.
     */
    // TODO: 2018/3/22 by zmyer
    private Map<String, Throttler> getThrottlers(StoreConfig config, Time time) {
        Map<String, Throttler> throttlers = new HashMap<>();
        // cleanup ops
        //创建清理ops节流器
        Throttler cleanupOpsThrottler = new Throttler(config.storeCleanupOperationsBytesPerSec, -1, true, time);
        throttlers.put(CLEANUP_OPS_JOB_NAME, cleanupOpsThrottler);
        // stats
        //创建索引扫描节流器
        Throttler statsIndexScanThrottler = new Throttler(config.storeStatsIndexEntriesPerSecond, 1000, true, time);
        throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, statsIndexScanThrottler);
        //返回结果
        return throttlers;
    }

    /**
     * @throws StoreException if the disk's mount path is inaccessible.
     */
    // TODO: 2018/3/22 by zmyer
    private void checkMountPathAccessible() throws StoreException {
        File mountPath = new File(disk.getMountPath());
        if (!mountPath.exists()) {
            metrics.diskMountPathFailures.inc();
            throw new StoreException("Mount path does not exist: " + mountPath + " ; cannot start stores on this disk",
                    StoreErrorCodes.Initialization_Error);
        }
    }
}
