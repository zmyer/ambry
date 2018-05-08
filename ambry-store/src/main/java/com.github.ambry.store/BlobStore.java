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

import com.codahale.metrics.Timer;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.WriteStatusDelegate;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * The blob store that controls the log and index
 */
// TODO: 2018/3/22 by zmyer
class BlobStore implements Store {
    //分隔符
    static final String SEPARATOR = "_";
    //锁文件后缀
    private final static String LockFile = ".lock";

    //存储对象id
    private final String storeId;
    //数据目录
    private final String dataDir;
    //任务调度器
    private final ScheduledExecutorService taskScheduler;
    //长任务调度器
    private final ScheduledExecutorService longLivedTaskScheduler;
    //磁盘IO调度器
    private final DiskIOScheduler diskIOScheduler;
    //磁盘空间分配器
    private final DiskSpaceAllocator diskSpaceAllocator;
    //日志对象
    private final Logger logger = LoggerFactory.getLogger(getClass());
    //存储写锁
    private final Object storeWriteLock = new Object();
    //存储配置
    private final StoreConfig config;
    //存储对象容量大小
    private final long capacityInBytes;
    //存储键值工厂对象
    private final StoreKeyFactory factory;
    //消息存储恢复对象
    private final MessageStoreRecovery recovery;
    //消息存储删除对象
    private final MessageStoreHardDelete hardDelete;
    //存储统计
    private final StoreMetrics metrics;
    private final StoreMetrics storeUnderCompactionMetrics;
    //计时器
    private final Time time;
    //会话id
    private final UUID sessionId = UUID.randomUUID();
    //副本id
    private final ReplicaId replicaId;
    //存储对象写状态代理
    private final WriteStatusDelegate writeStatusDelegate;
    //存储对象容量高水位
    private final long thresholdBytesHigh;
    //存储对象容量低水位
    private final long thresholdBytesLow;

    //存放消息文件对象
    private Log log;
    //存储空间压缩对象，主要用于删除一些无用的数据
    private BlobStoreCompactor compactor;
    //索引持久化对象
    private PersistentIndex index;
    //存储对象状态信息
    private BlobStoreStats blobStoreStats;
    //存储对象是否启动
    private boolean started;
    //文件锁
    private FileLock fileLock;

    /**
     * States representing the different scenarios that can occur when a set of messages are to be written to the store.
     * Nomenclature:
     * Absent: a key that is non-existent in the store.
     * Duplicate: a key that exists in the store and has the same CRC (meaning the message is the same).
     * Colliding: a key that exists in the store but has a different CRC (meaning the message is different).
     */
    // TODO: 2018/3/22 by zmyer
    private enum MessageWriteSetStateInStore {
        ALL_ABSENT,
        // The messages are all absent in the store.
        COLLIDING,
        // At least one message in the write set has the same key as another, different message in the store.
        ALL_DUPLICATE,
        // The messages are all duplicates - every one of them already exist in the store.
        SOME_NOT_ALL_DUPLICATE, // At least one of the message is a duplicate, but not all.
    }

    /**
     * Constructor for BlobStore, used in ambry-server
     * @param replicaId replica associated with BlobStore.  BlobStore id, data directory, and capacity derived from this
     * @param config the settings for store configuration.
     * @param taskScheduler the {@link ScheduledExecutorService} for executing short period background tasks.
     * @param longLivedTaskScheduler the {@link ScheduledExecutorService} for executing long period background tasks.
     * @param diskIOScheduler schedules disk IO operations
     * @param diskSpaceAllocator allocates log segment files.
     * @param metrics the {@link StorageManagerMetrics} instance to use.
     * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
     * @param factory the {@link StoreKeyFactory} for parsing store keys.
     * @param recovery the {@link MessageStoreRecovery} instance to use.
     * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
     * @param writeStatusDelegate delegate used to communicate BlobStore write status (sealed or unsealed)
     * @param time the {@link Time} instance to use.
     */
    // TODO: 2018/3/22 by zmyer
    BlobStore(ReplicaId replicaId, StoreConfig config, ScheduledExecutorService taskScheduler,
            ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
            DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
            StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete,
            WriteStatusDelegate writeStatusDelegate, Time time) {
        this(replicaId, replicaId.getPartitionId().toString(), config, taskScheduler, longLivedTaskScheduler,
                diskIOScheduler, diskSpaceAllocator, metrics, storeUnderCompactionMetrics, replicaId.getReplicaPath(),
                replicaId.getCapacityInBytes(), factory, recovery, hardDelete, writeStatusDelegate, time);
    }

    /**
     * Constructor for BlobStore, used in ambry-tools
     * @param storeId id of the BlobStore
     * @param config the settings for store configuration.
     * @param taskScheduler the {@link ScheduledExecutorService} for executing background tasks.
     * @param longLivedTaskScheduler the {@link ScheduledExecutorService} for executing long period background tasks.
     * @param diskIOScheduler schedules disk IO operations.
     * @param diskSpaceAllocator allocates log segment files.
     * @param metrics the {@link StorageManagerMetrics} instance to use.
     * @param storeUnderCompactionMetrics the {@link StoreMetrics} object used by stores created for compaction.
     * @param dataDir directory that will be used by the BlobStore for data
     * @param capacityInBytes capacity of the BlobStore
     * @param factory the {@link StoreKeyFactory} for parsing store keys.
     * @param recovery the {@link MessageStoreRecovery} instance to use.
     * @param hardDelete the {@link MessageStoreHardDelete} instance to use.
     * @param time the {@link Time} instance to use.
     */
    // TODO: 2018/3/22 by zmyer
    BlobStore(String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
            ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
            DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
            String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
            MessageStoreHardDelete hardDelete, Time time) {
        this(null, storeId, config, taskScheduler, longLivedTaskScheduler, diskIOScheduler, diskSpaceAllocator, metrics,
                storeUnderCompactionMetrics, dataDir, capacityInBytes, factory, recovery, hardDelete, null, time);
    }

    // TODO: 2018/3/22 by zmyer
    private BlobStore(ReplicaId replicaId, String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
            ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
            DiskSpaceAllocator diskSpaceAllocator, StoreMetrics metrics, StoreMetrics storeUnderCompactionMetrics,
            String dataDir, long capacityInBytes, StoreKeyFactory factory, MessageStoreRecovery recovery,
            MessageStoreHardDelete hardDelete, WriteStatusDelegate writeStatusDelegate, Time time) {
        this.replicaId = replicaId;
        this.storeId = storeId;
        this.dataDir = dataDir;
        this.taskScheduler = taskScheduler;
        this.longLivedTaskScheduler = longLivedTaskScheduler;
        this.diskIOScheduler = diskIOScheduler;
        this.diskSpaceAllocator = diskSpaceAllocator;
        this.metrics = metrics;
        this.storeUnderCompactionMetrics = storeUnderCompactionMetrics;
        this.config = config;
        this.capacityInBytes = capacityInBytes;
        this.factory = factory;
        this.recovery = recovery;
        this.hardDelete = hardDelete;
        this.writeStatusDelegate = config.storeWriteStatusDelegateEnable ? writeStatusDelegate : null;
        this.time = time;
        long threshold = config.storeReadOnlyEnableSizeThresholdPercentage;
        long delta = config.storeReadWriteEnableSizeThresholdPercentageDelta;
        this.thresholdBytesHigh = (long) (capacityInBytes * (threshold / 100.0));
        this.thresholdBytesLow = (long) (capacityInBytes * ((threshold - delta) / 100.0));
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public void start() throws StoreException {
        //加锁
        synchronized (storeWriteLock) {
            if (started) {
                //存储对象已经启动
                throw new StoreException("Store already started", StoreErrorCodes.Store_Already_Started);
            }
            final Timer.Context context = metrics.storeStartTime.time();
            try {
                // Check if the data dir exist. If it does not exist, create it
                //检查数据目录是否存在，不存在则需要创建
                File dataFile = new File(dataDir);
                if (!dataFile.exists()) {
                    logger.info("Store : {} data directory not found. creating it", dataDir);
                    boolean created = dataFile.mkdir();
                    if (!created) {
                        throw new StoreException("Failed to create directory for data dir " + dataDir,
                                StoreErrorCodes.Initialization_Error);
                    }
                }
                if (!dataFile.isDirectory() || !dataFile.canRead()) {
                    throw new StoreException(
                            dataFile.getAbsolutePath() + " is either not a directory or is not readable",
                            StoreErrorCodes.Initialization_Error);
                }

                // lock the directory
                //锁住数据目录
                fileLock = new FileLock(new File(dataDir, LockFile));
                if (!fileLock.tryLock()) {
                    throw new StoreException(
                            "Failed to acquire lock on file " + dataDir +
                                    ". Another process or thread is using this directory.",
                            StoreErrorCodes.Initialization_Error);
                }

                //创建存储描述符对象
                StoreDescriptor storeDescriptor = new StoreDescriptor(dataDir);

                //创建log对象
                log = new Log(dataDir, capacityInBytes, config.storeSegmentSizeInBytes, diskSpaceAllocator, metrics);

                //创建存储压缩对象
                compactor = new BlobStoreCompactor(dataDir, storeId, factory, config, metrics,
                        storeUnderCompactionMetrics,
                        diskIOScheduler, diskSpaceAllocator, log, time, sessionId, storeDescriptor.getIncarnationId());

                //创建索引持久化对象
                index = new PersistentIndex(dataDir, storeId, taskScheduler, log, config, factory, recovery, hardDelete,
                        diskIOScheduler, metrics, time, sessionId, storeDescriptor.getIncarnationId());

                //初始化压缩对象
                compactor.initialize(index);

                //初始化日志统计
                metrics.initializeIndexGauges(storeId, index, capacityInBytes);

                //删除消息间隔时间
                long logSegmentForecastOffsetMs = TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays);
                //
                long bucketSpanInMs = TimeUnit.MINUTES.toMillis(config.storeStatsBucketSpanInMinutes);
                //队列处理间隔周期
                long queueProcessingPeriodInMs =
                        TimeUnit.MINUTES.toMillis(config.storeStatsRecentEntryProcessingIntervalInMinutes);
                //存储对象状态信息
                blobStoreStats =
                        new BlobStoreStats(storeId, index, config.storeStatsBucketCount, bucketSpanInMs,
                                logSegmentForecastOffsetMs,
                                queueProcessingPeriodInMs, config.storeStatsWaitTimeoutInSecs, time,
                                longLivedTaskScheduler,
                                taskScheduler, diskIOScheduler, metrics);
                //检查存储容量以及变更存储对象写状态
                checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
                //设置启动标记
                started = true;
            } catch (Exception e) {
                metrics.storeStartFailure.inc();
                throw new StoreException("Error while starting store for dir " + dataDir, e,
                        StoreErrorCodes.Initialization_Error);
            } finally {
                context.stop();
            }
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
        checkStarted();
        // allows concurrent gets
        final Timer.Context context = metrics.getResponse.time();
        try {
            List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>(ids.size());
            Map<StoreKey, MessageInfo> indexMessages = new HashMap<StoreKey, MessageInfo>(ids.size());
            for (StoreKey key : ids) {
                //从索引信息中读取访问存储的配置信息
                BlobReadOptions readInfo = index.getBlobReadInfo(key, storeGetOptions);
                //将相关读取项插入到集合中
                readOptions.add(readInfo);
                //将读取项信息插入到索引消息列表中
                indexMessages.put(key, readInfo.getMessageInfo());
                // validate accountId and containerId
                //认证账号信息
                if (!validateAuthorization(readInfo.getMessageInfo().getAccountId(),
                        readInfo.getMessageInfo().getContainerId(),
                        key.getAccountId(), key.getContainerId())) {
                    if (config.storeValidateAuthorization) {
                        throw new StoreException(
                                "GET authorization failure. Key: " + key.getID() + " Actually accountId: " +
                                        readInfo.getMessageInfo()
                                                .getAccountId() + " Actually containerId: " +
                                        readInfo.getMessageInfo().getContainerId(),
                                StoreErrorCodes.Authorization_Failure);
                    } else {
                        logger.warn(
                                "GET authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                                key.getID(), readInfo.getMessageInfo().getAccountId(),
                                readInfo.getMessageInfo().getContainerId());
                        metrics.getAuthorizationFailureCount.inc();
                    }
                }
            }

            //创建消息读取集合
            MessageReadSet readSet = new StoreMessageReadSet(readOptions);
            // We ensure that the metadata list is ordered with the order of the message read set view that the
            // log provides. This ensures ordering of all messages across the log and metadata from the index.
            //消息信息集合
            List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(readSet.count());
            for (int i = 0; i < readSet.count(); i++) {
                //将消息读取信息插入到集合中
                messageInfoList.add(indexMessages.get(readSet.getKeyAt(i)));
            }
            //返回结果
            return new StoreInfo(readSet, messageInfoList);
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException("Unknown exception while trying to fetch blobs from store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
        }
    }

    /**
     * Checks the state of the messages in the given {@link MessageWriteSet} in the given {@link FileSpan}.
     * @param messageSetToWrite Non-empty set of messages to write to the store.
     * @param fileSpan The fileSpan on which the check for existence of the messages have to be made.
     * @return {@link MessageWriteSetStateInStore} representing the outcome of the state check.
     * @throws StoreException relays those encountered from {@link PersistentIndex#findKey(StoreKey, FileSpan)}.
     */
    // TODO: 2018/3/22 by zmyer
    private MessageWriteSetStateInStore checkWriteSetStateInStore(MessageWriteSet messageSetToWrite, FileSpan fileSpan)
            throws StoreException {
        int existingIdenticalEntries = 0;
        for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
            //首先在索引文件中查找具体的消息对应的键值
            if (index.findKey(info.getStoreKey(), fileSpan) != null) {
                if (index.wasRecentlySeen(info)) {
                    //如果当前的消息已经访问过，则递增访问次数
                    existingIdenticalEntries++;
                    metrics.identicalPutAttemptCount.inc();
                } else {
                    return MessageWriteSetStateInStore.COLLIDING;
                }
            }
        }
        if (existingIdenticalEntries == messageSetToWrite.getMessageSetInfo().size()) {
            //全部重复
            return MessageWriteSetStateInStore.ALL_DUPLICATE;
        } else if (existingIdenticalEntries > 0) {
            //部分重复
            return MessageWriteSetStateInStore.SOME_NOT_ALL_DUPLICATE;
        } else {
            //全部都是新的
            return MessageWriteSetStateInStore.ALL_ABSENT;
        }
    }

    /**
     * Checks the used capacity of the store against the configured percentage thresholds to see if the store
     * should be read-only or read-write
     * @param totalCapacity total capacity of the store in bytes
     * @param usedCapacity total used capacity of the store in bytes
     */
    // TODO: 2018/3/22 by zmyer
    private void checkCapacityAndUpdateWriteStatusDelegate(long totalCapacity, long usedCapacity) {
        if (writeStatusDelegate != null) {
            if (index.getLogUsedCapacity() > thresholdBytesHigh && !replicaId.isSealed()) {
                //如果当前的索引占用的空间已经超过了阈值，并且当前的副本不是只读的，则需要及时将副本改为只读模式
                if (!writeStatusDelegate.seal(replicaId)) {
                    //模式变更失败，则需要递增错误次数
                    metrics.sealSetError.inc();
                }
            } else if (index.getLogUsedCapacity() <= thresholdBytesLow && replicaId.isSealed()) {
                //如果当前的索引占用大小小于阈值，则需要及时将副本只读模式变更为读写模式
                if (!writeStatusDelegate.unseal(replicaId)) {
                    //递增失败次数
                    metrics.unsealSetError.inc();
                }
            }
            //else: maintain current replicaId status if percentFilled between threshold - delta and threshold
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.putResponse.time();
        try {
            //如果待写入的消息集合为空，则直接返回
            if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
                throw new IllegalArgumentException("Message write set cannot be empty");
            }

            //获取目前索引文件当前的写入偏移量
            Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
            //检查存储对象中写入状态信息
            MessageWriteSetStateInStore state = checkWriteSetStateInStore(messageSetToWrite, null);
            if (state == MessageWriteSetStateInStore.ALL_ABSENT) {
                //加锁
                synchronized (storeWriteLock) {
                    // Validate that log end offset was not changed. If changed, check once again for existing
                    // keys in store
                    //获取索引文件当前的写入偏移量
                    Offset currentIndexEndOffset = index.getCurrentEndOffset();
                    if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
                        //如果两个地方获得写入偏移量不相同，则说明有更改，此时需要再进行一次检查写入消息集合的在存储对象中的状态信息
                        FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
                        //检查写入消息集合的状态信息
                        state = checkWriteSetStateInStore(messageSetToWrite, fileSpan);
                    }

                    //如果此时写入消息集合都还是新的
                    if (state == MessageWriteSetStateInStore.ALL_ABSENT) {
                        //获取log文件的写入偏移量
                        Offset endOffsetOfLastMessage = log.getEndOffset();
                        //将需要写入的消息集合写入到log文件中
                        messageSetToWrite.writeTo(log);
                        logger.trace("Store : {} message set written to log", dataDir);

                        //获取所有写入消息信息
                        List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
                        ArrayList<IndexEntry> indexEntries = new ArrayList<>(messageInfo.size());
                        for (MessageInfo info : messageInfo) {
                            //依次遍历每个消息信息，为每个消息获取FileSpan对象，用于存放消息
                            FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
                            //创建索引信息
                            IndexValue value = new IndexValue(info.getSize(), fileSpan.getStartOffset(),
                                    info.getExpirationTimeInMs(),
                                    info.getOperationTimeMs(), info.getAccountId(), info.getContainerId());
                            //创建消息对应的索引项
                            IndexEntry entry = new IndexEntry(info.getStoreKey(), value, info.getCrc());
                            //将索引项插入到集合中
                            indexEntries.add(entry);
                            //更新logsegment写入偏移量信息
                            endOffsetOfLastMessage = fileSpan.getEndOffset();
                        }
                        //为消息索引数据创建FileSpan数据
                        FileSpan fileSpan = new FileSpan(indexEntries.get(0).getValue().getOffset(),
                                endOffsetOfLastMessage);
                        //将消息索引数据写入到indexSegment中
                        index.addToIndex(indexEntries, fileSpan);
                        for (IndexEntry newEntry : indexEntries) {
                            //更新存储对象存储信息
                            blobStoreStats.handleNewPutEntry(newEntry.getValue());
                        }
                        logger.trace("Store : {} message set written to index ", dataDir);
                        //检查存储对象容量以及更新写入状态
                        checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
                    }
                }
            }
            switch (state) {
            case COLLIDING:
                throw new StoreException(
                        "For at least one message in the write set, another blob with same key exists in store",
                        StoreErrorCodes.Already_Exist);
            case SOME_NOT_ALL_DUPLICATE:
                throw new StoreException(
                        "At least one message but not all in the write set is identical to an existing entry",
                        StoreErrorCodes.Already_Exist);
            case ALL_DUPLICATE:
                logger.trace("All entries to put already exist in the store, marking operation as successful");
                break;
            case ALL_ABSENT:
                logger.trace("All entries were absent, and were written to the store successfully");
                break;
            }
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException("IO error while trying to put blobs to store " + dataDir, e,
                    StoreErrorCodes.IOError);
        } catch (Exception e) {
            throw new StoreException("Unknown error while trying to put blobs to store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.deleteResponse.time();
        try {
            //待删除的索引项
            List<IndexValue> indexValuesToDelete = new ArrayList<>();
            //获取待删除的消息集合
            List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
            //获取当前indexSegment末尾偏移量
            Offset indexEndOffsetBeforeCheck = index.getCurrentEndOffset();
            for (MessageInfo info : infoList) {
                //根据提供的键，查找具体的索引信息
                IndexValue value = index.findKey(info.getStoreKey());
                if (value == null) {
                    //如果查找不到具体的索引信息，则直接异常
                    throw new StoreException(
                            "Cannot delete id " + info.getStoreKey() + " since it is not present in the index.",
                            StoreErrorCodes.ID_Not_Found);
                } else if (!validateAuthorization(value.getAccountId(), value.getContainerId(), info.getAccountId(),
                        info.getContainerId())) {
                    if (config.storeValidateAuthorization) {
                        throw new StoreException(
                                "DELETE authorization failure. Key: " + info.getStoreKey() + "Actually accountId: "
                                        + value.getAccountId() + "Actually containerId: " + value.getContainerId(),
                                StoreErrorCodes.Authorization_Failure);
                    } else {
                        logger.warn(
                                "DELETE authorization failure. Key: {} Actually accountId: {} Actually containerId: {}",
                                info.getStoreKey(), value.getAccountId(), value.getContainerId());
                        metrics.deleteAuthorizationFailureCount.inc();
                    }
                } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
                    throw new StoreException(
                            "Cannot delete id " + info.getStoreKey() + " since it is already deleted in the index.",
                            StoreErrorCodes.ID_Deleted);
                }
                //将待删除的索引值插入到集合中
                indexValuesToDelete.add(value);
            }
            synchronized (storeWriteLock) {
                //获取当前indexSegment可写偏移量
                Offset currentIndexEndOffset = index.getCurrentEndOffset();
                if (!currentIndexEndOffset.equals(indexEndOffsetBeforeCheck)) {
                    //如果两次检查不一致，则需要在两者之间再进行一次检查
                    FileSpan fileSpan = new FileSpan(indexEndOffsetBeforeCheck, currentIndexEndOffset);
                    for (MessageInfo info : infoList) {
                        //开始查找指定的键，对应的索引value
                        IndexValue value = index.findKey(info.getStoreKey(), fileSpan);
                        if (value != null && value.isFlagSet(IndexValue.Flags.Delete_Index)) {
                            throw new StoreException(
                                    "Cannot delete id " + info.getStoreKey() +
                                            " since it is already deleted in the index.",
                                    StoreErrorCodes.ID_Deleted);
                        }
                    }
                }
                //获取log对应的可写偏移量
                Offset endOffsetOfLastMessage = log.getEndOffset();
                //将待删除的消息写入到log文件中
                messageSetToDelete.writeTo(log);
                logger.trace("Store : {} delete mark written to log", dataDir);
                int correspondingPutIndex = 0;
                for (MessageInfo info : infoList) {
                    //为每个消息分配FileSpan空间
                    FileSpan fileSpan = log.getFileSpanForMessage(endOffsetOfLastMessage, info.getSize());
                    //标注该消息为待删除
                    IndexValue deleteIndexValue = index.markAsDeleted(info.getStoreKey(), fileSpan,
                            info.getOperationTimeMs());
                    //记录当前log的可写偏移量
                    endOffsetOfLastMessage = fileSpan.getEndOffset();
                    //更新统计信息
                    blobStoreStats.handleNewDeleteEntry(deleteIndexValue,
                            indexValuesToDelete.get(correspondingPutIndex++));
                }
                logger.trace("Store : {} delete has been marked in the index ", dataDir);
            }
        } catch (StoreException e) {
            throw e;
        } catch (IOException e) {
            throw new StoreException("IO error while trying to delete blobs from store " + dataDir, e,
                    StoreErrorCodes.IOError);
        } catch (Exception e) {
            throw new StoreException("Unknown error while trying to delete blobs from store " + dataDir, e,
                    StoreErrorCodes.Unknown_Error);
        } finally {
            context.stop();
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.findEntriesSinceResponse.time();
        try {
            //根据给定的查找键，查找指定数目的消息
            return index.findEntriesSince(token, maxTotalSizeOfEntries);
        } finally {
            context.stop();
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.findMissingKeysResponse.time();
        try {
            //查找丢失的key
            return index.findMissingKeys(keys);
        } finally {
            context.stop();
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public StoreStats getStoreStats() {
        return blobStoreStats;
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public boolean isKeyDeleted(StoreKey key) throws StoreException {
        checkStarted();
        final Timer.Context context = metrics.isKeyDeletedResponse.time();
        try {
            //根据key，获取对应的索引value信息
            IndexValue value = index.findKey(key);
            if (value == null) {
                throw new StoreException("Key " + key + " not found in store. Cannot check if it is deleted",
                        StoreErrorCodes.ID_Not_Found);
            }
            //检查当前索引value是否设置了删除标记
            return value.isFlagSet(IndexValue.Flags.Delete_Index);
        } finally {
            context.stop();
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public long getSizeInBytes() {
        return index.getLogUsedCapacity();
    }

    /**
     * Fetch {@link CompactionDetails} based on the {@link CompactionPolicy} for this {@link BlobStore} containing
     * information about log segments to be compacted
     * @param compactionPolicy the {@link CompactionPolicy} that needs to be used to determine the {@link CompactionDetails}
     * @return the {@link CompactionDetails} containing information about log segments to be compacted. Could be
     * {@code null} if there isn't anything to compact
     * @throws StoreException on any issues while reading entries from index
     */
    // TODO: 2018/3/22 by zmyer
    CompactionDetails getCompactionDetails(CompactionPolicy compactionPolicy) throws StoreException {
        return compactionPolicy.getCompactionDetails(capacityInBytes, index.getLogUsedCapacity(),
                log.getSegmentCapacity(),
                LogSegment.HEADER_SIZE, index.getLogSegmentsNotInJournal(), blobStoreStats);
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public void shutdown() throws StoreException {
        long startTimeInMs = time.milliseconds();
        synchronized (storeWriteLock) {
            checkStarted();
            try {
                logger.info("Store : " + dataDir + " shutting down");
                blobStoreStats.close();
                compactor.close(30);
                index.close();
                log.close();
                started = false;
            } catch (Exception e) {
                logger.error("Store : " + dataDir + " shutdown of store failed for directory ", e);
            } finally {
                try {
                    fileLock.destroy();
                } catch (IOException e) {
                    logger.error("Store : " + dataDir + " IO Exception while trying to close the file lock", e);
                }
                metrics.storeShutdownTimeInMs.update(time.milliseconds() - startTimeInMs);
            }
        }
    }

    /**
     * @return the {@link DiskSpaceRequirements} for this store to provide to
     * {@link DiskSpaceAllocator#initializePool(Collection)}. This will be {@code null} if this store uses a non-segmented
     * log. This is because it does not require any additional/swap segments.
     * @throws StoreException
     */
    // TODO: 2018/3/22 by zmyer
    DiskSpaceRequirements getDiskSpaceRequirements() throws StoreException {
        checkStarted();
        DiskSpaceRequirements requirements = log.isLogSegmented() ? new DiskSpaceRequirements(log.getSegmentCapacity(),
                log.getRemainingUnallocatedSegments(), compactor.getSwapSegmentsInUse()) : null;
        logger.debug("Store {} has disk space requirements: {}", storeId, requirements);
        return requirements;
    }

    /**
     * @return {@code true} if this store has been started successfully.
     */
    // TODO: 2018/3/22 by zmyer
    boolean isStarted() {
        return started;
    }

    /**
     * Compacts the store data based on {@code details}.
     * @param details the {@link CompactionDetails} describing what needs to be compacted.
     * @throws IllegalArgumentException if any of the provided segments doesn't exist in the log or if one or more offsets
     * in the segments to compact are in the journal.
     * @throws IOException if there is any error creating the {@link CompactionLog}.
     * @throws StoreException if there are any errors during the compaction.
     */
    // TODO: 2018/3/22 by zmyer
    void compact(CompactionDetails details) throws IOException, StoreException {
        checkStarted();
        compactor.compact(details);
        checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
    }

    /**
     * Resumes a compaction if one is in progress.
     * @throws StoreException if there are any errors during the compaction.
     */
    // TODO: 2018/3/22 by zmyer
    void maybeResumeCompaction() throws StoreException {
        checkStarted();
        if (CompactionLog.isCompactionInProgress(dataDir, storeId)) {
            logger.info("Resuming compaction of {}", this);
            compactor.resumeCompaction();
            checkCapacityAndUpdateWriteStatusDelegate(log.getCapacityInBytes(), index.getLogUsedCapacity());
        }
    }

    // TODO: 2018/3/22 by zmyer
    private void checkStarted() throws StoreException {
        if (!started) {
            throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
        }
    }

    // TODO: 2018/3/22 by zmyer
    @Override
    public String toString() {
        return "StoreId: " + storeId + ". DataDir: " + dataDir + ". Capacity: " + capacityInBytes;
    }

    /**
     * Check if accountId/containerId from store and request are same.
     * If either one of accountId and containerId in store is unknown, the validation is skipped.
     */
    // TODO: 2018/3/22 by zmyer
    private boolean validateAuthorization(short storeAccountId, short storeContainerId, short requestAccountId,
            short requestContainerId) {
        if (storeAccountId != Account.UNKNOWN_ACCOUNT_ID && storeContainerId != Container.UNKNOWN_CONTAINER_ID) {
            return (storeAccountId == requestAccountId) && (storeContainerId == requestContainerId);
        }
        return true;
    }
}
