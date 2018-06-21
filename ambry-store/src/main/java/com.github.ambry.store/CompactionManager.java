/**
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
package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Responsible for managing compaction of a {@link BlobStore}.
 */
// TODO: 2018/3/22 by zmyer
class CompactionManager {
    //线程前缀
    static final String THREAD_NAME_PREFIX = "StoreCompactionThread-";

    private enum Trigger {
        PERIODIC,
        ADMIN
    }

    //挂载点
    private final String mountPath;
    //存储配置
    private final StoreConfig storeConfig;
    //定时器
    private final Time time;
    //存储对象集合
    private final Collection<BlobStore> stores;
    //压缩执行器
    private final CompactionExecutor compactionExecutor;
    //存储管理器统计信息
    private final StorageManagerMetrics metrics;
    //压缩策略
    private final CompactionPolicy compactionPolicy;
    //日志对象
    private final Logger logger = LoggerFactory.getLogger(getClass());

    //压缩线程
    private Thread compactionThread;

    /**
     * Creates a CompactionManager that handles scheduling and executing compaction.
     * @param mountPath the mount path of all the stores for which compaction may be executed.
     * @param storeConfig the {@link StoreConfig} that contains configurationd details.
     * @param stores the {@link Collection} of {@link BlobStore} that compaction needs to be executed for.
     * @param metrics the {@link StorageManagerMetrics} to use.
     * @param time the {@link Time} instance to use.
     */
    // TODO: 2018/3/22 by zmyer
    CompactionManager(String mountPath, StoreConfig storeConfig, Collection<BlobStore> stores,
            StorageManagerMetrics metrics, Time time) {
        this.mountPath = mountPath;
        this.storeConfig = storeConfig;
        this.stores = stores;
        this.time = time;
        this.metrics = metrics;
        if (!storeConfig.storeCompactionTriggers[0].isEmpty()) {
            EnumSet<Trigger> triggers = EnumSet.noneOf(Trigger.class);
            for (String trigger : storeConfig.storeCompactionTriggers) {
                triggers.add(Trigger.valueOf(trigger.toUpperCase()));
            }
            compactionExecutor = new CompactionExecutor(triggers);
            try {
                CompactionPolicyFactory compactionPolicyFactory =
                        Utils.getObj(storeConfig.storeCompactionPolicyFactory, storeConfig, time);
                compactionPolicy = compactionPolicyFactory.getCompactionPolicy();
            } catch (Exception e) {
                throw new IllegalStateException("Error creating compaction policy using compactionPolicyFactory "
                        + storeConfig.storeCompactionPolicyFactory);
            }
        } else {
            compactionExecutor = null;
            compactionPolicy = null;
        }
    }

    /**
     * Enables the compaction manager allowing it execute compactions if required.
     */
    // TODO: 2018/5/15 by zmyer
    void enable() {
        if (compactionExecutor != null) {
            logger.info("Compaction thread started for {}", mountPath);
            compactionThread = Utils.newThread(THREAD_NAME_PREFIX + mountPath, compactionExecutor, true);
            compactionThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.error("Thread {} threw exception", t, e);
                }
            });
            compactionThread.start();
        }
    }

    /**
     * Disables the compaction manager which disallows any new compactions.
     */
    void disable() {
        if (compactionExecutor != null) {
            compactionExecutor.disable();
        }
    }

    /**
     * Awaits the termination of all pending jobs of the compaction manager.
     */
    void awaitTermination() {
        if (compactionExecutor != null && compactionThread != null) {
            try {
                compactionThread.join(2000);
            } catch (InterruptedException e) {
                metrics.compactionManagerTerminateErrorCount.inc();
                logger.error("Compaction thread join wait for {} was interrupted", mountPath);
            }
        }
    }

    /**
     * @return {@code true} if the compaction thread is running. {@code false} otherwise.
     */
    boolean isCompactionExecutorRunning() {
        return compactionExecutor != null && compactionExecutor.isRunning;
    }

    /**
     * Schedules the given {@code store} for compaction next.
     * @param store the {@link BlobStore} to compact.
     * @return {@code true} if the scheduling was successful. {@code false} if not.
     */
    // TODO: 2018/4/23 by zmyer
    boolean scheduleNextForCompaction(BlobStore store) {
        return compactionExecutor != null && compactionExecutor.scheduleNextForCompaction(store);
    }

    /**
     * Disable the given {@code store} for compaction.
     * @param store the {@link BlobStore} to be disabled.
     * @return {@code true} if the disabling was successful. {@code false} if not.
     */
    // TODO: 2018/4/23 by zmyer
    boolean disableCompactionForBlobStore(BlobStore store) {
        return compactionExecutor == null || compactionExecutor.disableCompactionForBlobStore(store);
    }

    /**
     * Get compaction details for a given {@link BlobStore} if any
     * @param blobStore the {@link BlobStore} for which compation details are requested
     * @return the {@link CompactionDetails} containing the details about log segments that needs to be compacted.
     * {@code null} if compaction is not required
     * @throws StoreException when {@link BlobStore} is not started
     */
    // TODO: 2018/5/15 by zmyer
    private CompactionDetails getCompactionDetails(BlobStore blobStore) throws StoreException {
        return blobStore.getCompactionDetails(compactionPolicy);
    }

    /**
     * A {@link Runnable} that cycles through the stores and executes compaction if required.
     */
    // TODO: 2018/5/15 by zmyer
    private class CompactionExecutor implements Runnable {
        //触发器集合
        private final EnumSet<Trigger> triggers;
        //日志对象
        private final Logger logger = LoggerFactory.getLogger(getClass());
        //重入锁
        private final ReentrantLock lock = new ReentrantLock();
        //条件变量
        private final Condition waitCondition = lock.newCondition();
        //跳过的储存对象集合
        private final Set<BlobStore> storesToSkip = new HashSet<>();
        //无法压缩的粗存储对象集合
        private final Set<BlobStore> storesDisabledCompaction = ConcurrentHashMap.newKeySet();
        //待检测存储对象队列
        private final LinkedBlockingDeque<BlobStore> storesToCheck = new LinkedBlockingDeque<>();
        //检查频率
        private final long waitTimeMs = TimeUnit.HOURS.toMillis(storeConfig.storeCompactionCheckFrequencyInHours);
        //是否开启压缩检查
        private volatile boolean enabled = true;
        //压缩线程是否运行
        volatile boolean isRunning = false;

        /**
         * @param triggers the {@link EnumSet} of active compaction triggers.
         */
        // TODO: 2018/5/15 by zmyer
        CompactionExecutor(EnumSet<Trigger> triggers) {
            this.triggers = triggers;
        }

        /**
         * Starts by resuming any compactions that were left halfway. In steady state, it cycles through the stores at a
         * configurable frequency and runs compaction as required.
         */
        @Override
        public void run() {
            isRunning = true;
            try {
                logger.info("Starting compaction thread for {}", mountPath);
                // complete any compactions in progress
                for (BlobStore store : stores) {
                    logger.trace("{} being checked for resume", store);
                    if (store.isStarted()) {
                        logger.trace("{} is started and eligible for resume check", store);
                        metrics.markCompactionStart(false);
                        try {
                            //恢复压缩存储对象
                            store.maybeResumeCompaction();
                        } catch (Exception e) {
                            metrics.compactionErrorCount.inc();
                            logger.error("Compaction of store {} failed on resume. Continuing with the next store",
                                    store, e);
                            //将压缩失败的存储对象插入到跳过列表中
                            storesToSkip.add(store);
                        } finally {
                            metrics.markCompactionStop();
                        }
                    }
                }
                // continue to do compactions as required.
                long expectedNextCheckTime = time.milliseconds() + waitTimeMs;
                if (triggers.contains(Trigger.PERIODIC)) {
                    //将所有的存储对象插入到待检测列表中
                    storesToCheck.addAll(stores);
                }
                while (enabled) {
                    try {
                        while (enabled && storesToCheck.peek() != null) {
                            //取出待压缩的存储对象
                            BlobStore store = storesToCheck.poll();
                            logger.trace("{} being checked for compaction", store);
                            boolean compactionStarted = false;
                            try {
                                if (store.isStarted() && !storesToSkip.contains(store) &&
                                        !storesDisabledCompaction.contains(store)) {
                                    logger.info("{} is started and is being checked for compaction eligibility", store);
                                    //获取存储对象的压缩详情
                                    CompactionDetails details = getCompactionDetails(store);
                                    if (details != null) {
                                        logger.trace("Generated {} as details for {}", details, store);
                                        metrics.markCompactionStart(true);
                                        compactionStarted = true;
                                        //开始针对存储对象进行压缩操作
                                        store.compact(details);
                                    } else {
                                        logger.info("{} is not eligible for compaction", store);
                                    }
                                }
                            } catch (Exception e) {
                                metrics.compactionErrorCount.inc();
                                logger.error("Compaction of store {} failed. Continuing with the next store", store, e);
                                storesToSkip.add(store);
                            } finally {
                                if (compactionStarted) {
                                    metrics.markCompactionStop();
                                }
                            }
                        }
                        lock.lock();
                        try {
                            if (enabled) {
                                if (storesToCheck.peek() == null) {
                                    if (triggers.contains(Trigger.PERIODIC)) {
                                        long actualWaitTimeMs = expectedNextCheckTime - time.milliseconds();
                                        logger.trace("Going to wait for {} ms in compaction thread at {}",
                                                actualWaitTimeMs, mountPath);
                                        time.await(waitCondition, actualWaitTimeMs);
                                    } else {
                                        logger.trace("Going to wait until compaction thread at {} is woken up",
                                                mountPath);
                                        waitCondition.await();
                                    }
                                }
                                if (triggers.contains(Trigger.PERIODIC) &&
                                        time.milliseconds() >= expectedNextCheckTime) {
                                    expectedNextCheckTime = time.milliseconds() + waitTimeMs;
                                    storesToCheck.addAll(stores);
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        metrics.compactionExecutorErrorCount.inc();
                        logger.error("Compaction executor for {} encountered an error. Continuing", mountPath, e);
                    }
                }
            } finally {
                isRunning = false;
                logger.info("Stopping compaction thread for {}", mountPath);
            }
        }

        /**
         * Disables the executor by disallowing scheduling of any new compaction jobs.
         */
        void disable() {
            lock.lock();
            try {
                enabled = false;
                waitCondition.signal();
            } finally {
                lock.unlock();
            }
            logger.info("Disabled compaction thread for {}", mountPath);
        }

        /**
         * Schedules the given {@code store} for compaction next.
         * @param store the {@link BlobStore} to compact.
         * @return {@code true} if the scheduling was successful. {@code false} if not.
         */
        // TODO: 2018/4/23 by zmyer
        boolean scheduleNextForCompaction(BlobStore store) {
            if (!enabled || !triggers.contains(Trigger.ADMIN) || !store.isStarted() || storesToSkip.contains(store)
                    || storesDisabledCompaction.contains(store)) {
                return false;
            }
            lock.lock();
            try {
                storesToCheck.addFirst(store);
                waitCondition.signal();
                logger.info("Scheduled {} for compaction", store);
            } finally {
                lock.unlock();
            }
            return true;
        }

        /**
         * Disable the compaction on given blobstore
         * @param store the {@link BlobStore} on which the compaction is disabled.
         * @return {@code true} if the disabling was successful. {@code false} if not.
         */
        boolean disableCompactionForBlobStore(BlobStore store) {
            storesDisabledCompaction.add(store);
            return true;
        }
    }
}

