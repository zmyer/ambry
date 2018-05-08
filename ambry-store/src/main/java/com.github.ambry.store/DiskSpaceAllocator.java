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

package com.github.ambry.store;

import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;


/**
 * {@link DiskSpaceAllocator} handles the allocation of disk space to entities that require disk segments, such as the
 * {@link BlobStore}s and {@link BlobStoreCompactor}s. The disk segments will be preallocated on platforms that support
 * the fallocate syscall. The entities that require disk space register there requirements using the
 * {@link #initializePool(Collection)} method. Disk space is allocated using the
 * {@link DiskSpaceAllocator#allocate(File, long)} method. If a segment matching the requested size is not in the pool,
 * it will be created at runtime. Disk space is returned to the pool using the
 * {@link DiskSpaceAllocator#free(File, long)} method. It is up to the requester to keep track of the size of the
 * segment requested.
 *
 * NOTE: Segments can be allocated and freed before pool initialization.  This will fall back to preallocating at
 * runtime if the current pool does not include a segment of the correct size.
 */
// TODO: 2018/3/19 by zmyer
class DiskSpaceAllocator {
    //日志对象
    private static final Logger logger = LoggerFactory.getLogger(DiskSpaceAllocator.class);
    //保存文件前缀
    private static final String RESERVE_FILE_PREFIX = "reserve_";
    //文件大小目录前缀
    private static final String FILE_SIZE_DIR_PREFIX = "reserve_size_";
    //文件过滤器
    private static final FileFilter RESERVE_FILE_FILTER =
            pathname -> pathname.isFile() && pathname.getName().startsWith(RESERVE_FILE_PREFIX);
    //文件大小目录过滤器
    private static final FileFilter FILE_SIZE_DIR_FILTER =
            pathname -> pathname.isDirectory() && getFileSizeForDirName(pathname.getName()) != null;
    //是否可以池化
    private final boolean enablePooling;
    //保留文件目录
    private final File reserveDir;
    //
    private final long requiredSwapSegmentsPerSize;
    //存储管理器统计信息
    private final StorageManagerMetrics metrics;
    //保留文件集合
    private final ReserveFileMap reserveFiles = new ReserveFileMap();
    //池状态
    private PoolState poolState = PoolState.NOT_INVENTORIED;
    //异常对象
    private Exception inventoryException = null;

    /**
     * This constructor will inventory any existing reserve files in the pool (adding them to the in-memory map) and
     * create the reserve directory if it does not exist. Any files that already exist in the pool can be checked out
     * prior to calling {@link #initializePool(Collection)}. If a file of the correct size does not yet exist, a new file
     * will be allocated at runtime.
     * @param enablePooling if set to {@code false}, the reserve pool will not be initialized/used and new files will be
     *                      created each time a segment is allocated.
     * @param reserveDir the directory where reserve files will reside. If this directory does not exist yet, it will
     *                   be created. This can be {@code null} if pooling is disabled.
     * @param requiredSwapSegmentsPerSize the number of swap segments needed for each segment size in the pool.
     * @param metrics a {@link StorageManagerMetrics} instance.
     * @throws StoreException
     */
    // TODO: 2018/3/22 by zmyer
    DiskSpaceAllocator(boolean enablePooling, File reserveDir, long requiredSwapSegmentsPerSize,
            StorageManagerMetrics metrics) {
        this.enablePooling = enablePooling;
        this.reserveDir = reserveDir;
        this.requiredSwapSegmentsPerSize = requiredSwapSegmentsPerSize;
        this.metrics = metrics;
        try {
            if (enablePooling) {
                //创建保留的文件目录
                prepareDirectory(reserveDir);
                //清查已经存在的保留的文件信息
                inventoryExistingReserveFiles();
                //设置池的状态为已清查
                poolState = PoolState.INVENTORIED;
            }
        } catch (Exception e) {
            inventoryException = e;
            logger.error("Could not inventory preexisting reserve directory", e);
        }
    }

    /**
     * This method will initialize the pool such that it matches the {@link DiskSpaceRequirements} passed in.
     * The following steps are performed:
     * <ol>
     *   <li>
     *     The requirements list is aggregated into an overall requirements map. This stage will ensure that the swap
     *     segments requirements per each segment size described in a DiskSpaceRequirements struct (even if a blob store
     *     needs 0 additional non-swap segments). The number of swap segments needed is calculated by counting the total
     *     number of swap segments allocated to the stores (as described by the disk space requirements). If the number
     *     of swap segments allocated is less than the required swap segment count (passed in via configuration) for a
     *     certain segment size, the difference is added into the overall requirements map. This ensures that the total
     *     (allocated and pooled) swap segment count equals the required count.
     *   </li>
     *   <li>
     *     Unneeded segments are deleted. These are segments that were present in the pool on disk, but are not needed
     *     according to the overall requirements.
     *   </li>
     *   <li>
     *     Any additional required segments are added. For each segment size, additional files will be preallocated to
     *     until the number of files at that size match the overall requirements map. If fallocate is supported on the
     *     system, and the call fails, pool initialization will fail.
     *   </li>
     * </ol>
     * WARNING: No calls to {@link #allocate(File, long)} and {@link #free(File, long)} may occur while this method is
     *          being executed.
     * @param requirementsList a collection of {@link DiskSpaceRequirements}s objects that describe the number and
     *                         segment size needed by each store.
     * @throws StoreException if the pool could not be allocated to meet the provided requirements
     */
    // TODO: 2018/3/22 by zmyer
    void initializePool(Collection<DiskSpaceRequirements> requirementsList) throws StoreException {
        long startTime = System.currentTimeMillis();
        try {
            if (enablePooling) {
                //如果池的状态还是未清理的，则直接异常
                if (poolState == PoolState.NOT_INVENTORIED) {
                    throw inventoryException;
                }
                //整合所有的磁盘空间需求
                Map<Long, Long> overallRequirements = getOverallRequirements(requirementsList);
                //删除不需要的segment对象
                deleteExtraSegments(overallRequirements);
                //添加新增的segment
                addRequiredSegments(overallRequirements);
                // TODO fill the disk with additional swap segments
                //设置池的状态为已初始化
                poolState = PoolState.INITIALIZED;
            } else {
                logger.info("Disk segment pooling disabled; pool will not be initialized.");
            }
        } catch (Exception e) {
            metrics.diskSpaceAllocatorInitFailureCount.inc();
            poolState = PoolState.NOT_INVENTORIED;
            throw new StoreException("Exception while initializing DiskSpaceAllocator pool", e,
                    StoreErrorCodes.Initialization_Error);
        } finally {
            //统计初始化池的耗时
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.info("initializePool took {} ms", elapsedTime);
            metrics.diskSpaceAllocatorStartTimeMs.update(elapsedTime);
        }
    }

    /**
     * Allocate a file, that is, take a file matching the requested size from the reserve directory and move it to the
     * provided file path
     * @param destinationFile the file path to move the allocated file to.
     * @param sizeInBytes the size in bytes of the requested file.
     * @throws IOException if the file could not be moved to the destination.
     */
    // TODO: 2018/3/22 by zmyer
    void allocate(File destinationFile, long sizeInBytes) throws IOException {
        long startTime = System.currentTimeMillis();
        try {
            if (enablePooling && poolState != PoolState.INITIALIZED) {
                logger.info("Allocating segment of size {} to {} before pool is fully initialized", sizeInBytes,
                        destinationFile.getAbsolutePath());
                metrics.diskSpaceAllocatorAllocBeforeInitCount.inc();
            } else {
                logger.debug("Allocating segment of size {} to {}", sizeInBytes, destinationFile.getAbsolutePath());
            }
            //目标文件已经存在
            if (destinationFile.exists()) {
                throw new IOException("Destination file already exists: " + destinationFile.getAbsolutePath());
            }
            File reserveFile = null;
            if (poolState != PoolState.NOT_INVENTORIED) {
                //根据文件大小，取出一个可用的文件
                reserveFile = reserveFiles.remove(sizeInBytes);
            }
            //如果不存在已保留的文件，则需要重新分配
            if (reserveFile == null) {
                if (enablePooling) {
                    logger.info(
                            "Segment of size {} not found in pool; attempting to create a new preallocated file; poolState: {}",
                            sizeInBytes, poolState);
                    metrics.diskSpaceAllocatorSegmentNotFoundCount.inc();
                }
                //需要重新分配文件
                Utils.preAllocateFileIfNeeded(destinationFile, sizeInBytes);
            } else {
                try {
                    //将分配到的文件移动到目标目录下
                    Files.move(reserveFile.toPath(), destinationFile.toPath());
                } catch (Exception e) {
                    reserveFiles.add(sizeInBytes, reserveFile);
                    throw e;
                }
            }
        } finally {
            //统计分配耗时
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.debug("allocate took {} ms", elapsedTime);
            metrics.diskSpaceAllocatorAllocTimeMs.update(elapsedTime);
        }
    }

    /**
     * Return a file to the pool. The user must keep track of the size of the file allocated.
     * @param fileToReturn the file to return to the pool.
     * @param sizeInBytes the size of the file to return.
     * @throws IOException if the file to return does not exist or cannot be cleaned or recreated correctly.
     */
    // TODO: 2018/3/22 by zmyer
    void free(File fileToReturn, long sizeInBytes) throws IOException {
        long startTime = System.currentTimeMillis();
        try {
            if (enablePooling && poolState != PoolState.INITIALIZED) {
                logger.info("Freeing segment of size {} from {} before pool is fully initialized", sizeInBytes,
                        fileToReturn.getAbsolutePath());
                metrics.diskSpaceAllocatorFreeBeforeInitCount.inc();
            } else {
                logger.debug("Freeing segment of size {} from {}", sizeInBytes, fileToReturn.getAbsolutePath());
            }
            // For now, we delete the file and create a new one. Newer linux kernel versions support
            // additional fallocate flags, which will be useful for cleaning up returned files.
            //删除文件
            Files.delete(fileToReturn.toPath());
            if (poolState == PoolState.INITIALIZED) {
                //保留指定大小的文件
                fileToReturn = createReserveFile(sizeInBytes);
                //将文件插入到集合中
                reserveFiles.add(sizeInBytes, fileToReturn);
            }
        } finally {
            long elapsedTime = System.currentTimeMillis() - startTime;
            logger.debug("free took {} ms", elapsedTime);
            metrics.diskSpaceAllocatorFreeTimeMs.update(elapsedTime);
        }
    }

    /**
     * Inventory existing reserve directories and add entries to {@link #reserveFiles}
     * @return a populated {@link ReserveFileMap}
     */
    // TODO: 2018/3/22 by zmyer
    private void inventoryExistingReserveFiles() throws IOException {
        //获取所有的文件大小目录
        File[] fileSizeDirs = reserveDir.listFiles(FILE_SIZE_DIR_FILTER);
        if (fileSizeDirs == null) {
            throw new IOException("Error while listing directories in " + reserveDir.getAbsolutePath());
        }
        for (File fileSizeDir : fileSizeDirs) {
            //从目录名中获取文件大小
            long sizeInBytes = getFileSizeForDirName(fileSizeDir.getName());
            //从目录中获取保留的文件
            File[] reserveFilesForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
            if (reserveFilesForSize == null) {
                throw new IOException("Error while listing files in " + fileSizeDir.getAbsolutePath());
            }
            for (File reserveFile : reserveFilesForSize) {
                //将文件对象以及文件信息插入到保留的文件集合中
                reserveFiles.add(sizeInBytes, reserveFile);
            }
        }
    }

    /**
     * Iterates over the provided requirements and creates a map that describes the number of segments that need to be in
     * the pool for each segment size.
     * @param requirementsList the collection of {@link DiskSpaceRequirements} objects to be accumulated
     * @return a {@link Map} from segment sizes to the number of reserve segments needed at that size.
     */
    // TODO: 2018/3/22 by zmyer
    private Map<Long, Long> getOverallRequirements(Collection<DiskSpaceRequirements> requirementsList) {
        Map<Long, Long> overallRequirements = new HashMap<>();
        Map<Long, Long> swapSegmentsUsed = new HashMap<>();

        for (DiskSpaceRequirements requirements : requirementsList) {
            //读取每个segment所需要占用空间
            long segmentSizeInBytes = requirements.getSegmentSizeInBytes();
            //统计还需要分配segment的数目
            overallRequirements.put(segmentSizeInBytes,
                    overallRequirements.getOrDefault(segmentSizeInBytes, 0L) + requirements.getSegmentsNeeded());
            //统计在swap中已经使用的segment的数目
            swapSegmentsUsed.put(segmentSizeInBytes,
                    swapSegmentsUsed.getOrDefault(segmentSizeInBytes, 0L) + requirements.getSwapSegmentsInUse());
        }
        for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : overallRequirements.entrySet()) {
            //获取segment的大小
            long sizeInBytes = sizeAndSegmentsNeeded.getKey();
            //获取所需要的segment的数目
            long origSegmentsNeeded = sizeAndSegmentsNeeded.getValue();
            // Ensure that swap segments are only added to the requirements if the number of swap segments allocated to
            // stores is lower than the minimum required swap segments.
            //计算还需要segment的数目
            long swapSegmentsNeeded = Math.max(requiredSwapSegmentsPerSize - swapSegmentsUsed.get(sizeInBytes), 0L);
            //将该需求插入到统一空间需求列表中
            overallRequirements.put(sizeInBytes, origSegmentsNeeded + swapSegmentsNeeded);
        }
        //返回结果
        return Collections.unmodifiableMap(overallRequirements);
    }

    /**
     * Delete the currently-present reserve files that are not required by {@code overallRequirements}.
     * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
     * @throws IOException
     */
    // TODO: 2018/3/22 by zmyer
    private void deleteExtraSegments(Map<Long, Long> overallRequirements) throws IOException {
        for (long sizeInBytes : reserveFiles.getFileSizeSet()) {
            //获取指定大小的segment所需要的个数
            Long segmentsNeeded = overallRequirements.get(sizeInBytes);
            if (segmentsNeeded == null || segmentsNeeded == 0) {
                //如果目前还不需要这类大小的segment,可以将该文件或者目录删除掉
                File dirToDelete = new File(reserveDir, generateFileSizeDirName(sizeInBytes));
                //删除掉指定的文件或者目录
                Utils.deleteFileOrDirectory(dirToDelete);
            } else {
                while (reserveFiles.getCount(sizeInBytes) > segmentsNeeded) {
                    //如果指定大小的文件保留个数大于需求的话，需要删除指定数目的保留文件
                    File fileToDelete = reserveFiles.remove(sizeInBytes);
                    if (!fileToDelete.delete()) {
                        //删除失败，异常处理
                        throw new IOException(
                                "Could not delete the following reserve file: " + fileToDelete.getAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Add additional reserve files that are required by {@code overallRequirements}.
     * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
     * @throws IOException
     */
    // TODO: 2018/3/22 by zmyer
    private void addRequiredSegments(Map<Long, Long> overallRequirements) throws IOException {
        for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : overallRequirements.entrySet()) {
            //所需segment的大小
            long sizeInBytes = sizeAndSegmentsNeeded.getKey();
            //所需segment数目
            long segmentsNeeded = sizeAndSegmentsNeeded.getValue();
            while (reserveFiles.getCount(sizeInBytes) < segmentsNeeded) {
                //如果指定大小的segment数目不够，则需要新增相应数量的segment
                reserveFiles.add(sizeInBytes, createReserveFile(sizeInBytes));
            }
        }
    }

    /**
     * Create and preallocate (if supported) a reserve file of the specified size.
     * @param sizeInBytes the size to preallocate for the reserve file.
     * @return the created file.
     * @throws IOException if the file could not be created, or if an error occured during the fallocate call.
     */
    // TODO: 2018/3/22 by zmyer
    private File createReserveFile(long sizeInBytes) throws IOException {
        //首先需要根据文件大小创建指定的文件目录
        File fileSizeDir = prepareDirectory(new File(reserveDir, FILE_SIZE_DIR_PREFIX + sizeInBytes));
        File reserveFile;
        do {
            //在文件目录中，创建保留文件
            reserveFile = new File(fileSizeDir, generateFilename());
        } while (!reserveFile.createNewFile());
        //为保留的文件分配磁盘空间
        Utils.preAllocateFileIfNeeded(reserveFile, sizeInBytes);
        return reserveFile;
    }

    /**
     * Create a directory if it does not yet exist and check that it is accessible.
     * @param dir The directory to possibly create.
     * @return the passed-in directory
     * @throws IOException if the specified directory could not be created/read, or if a non-directory file with the same
     *                     name already exists.
     */
    // TODO: 2018/3/22 by zmyer
    private static File prepareDirectory(File dir) throws IOException {
        if (!dir.exists()) {
            dir.mkdir();
        }
        if (!dir.isDirectory()) {
            throw new IOException(dir.getAbsolutePath() + " is not a directory or could not be created");
        }
        return dir;
    }

    /**
     * @param sizeInBytes the size of the files in this directory
     * @return a directory name for this size. This is reserve_size_{n} where {n} is {@code sizeInBytes}
     */
    // TODO: 2018/3/22 by zmyer
    static String generateFileSizeDirName(long sizeInBytes) {
        return FILE_SIZE_DIR_PREFIX + sizeInBytes;
    }

    /**
     * @return a filename for a reserve file. This is reserve_ followed by a random UUID.
     */
    // TODO: 2018/3/22 by zmyer
    private static String generateFilename() {
        return RESERVE_FILE_PREFIX + UUID.randomUUID();
    }

    /**
     * Parse the file size from a file size directory name.
     * @param fileSizeDirName the name of the file size directory.
     * @return the parsed size in bytes, or {@code null} if the directory name did not start with the correct prefix.
     * @throws NumberFormatException if a valid long does not follow the filename prefix.
     */
    // TODO: 2018/3/22 by zmyer
    private static Long getFileSizeForDirName(String fileSizeDirName) {
        Long sizeInBytes = null;
        if (fileSizeDirName.startsWith(FILE_SIZE_DIR_PREFIX)) {
            //获取空间大小
            String sizeString = fileSizeDirName.substring(FILE_SIZE_DIR_PREFIX.length());
            sizeInBytes = Long.parseLong(sizeString);
        }
        return sizeInBytes;
    }

    /**
     * This is a thread safe data structure that is used to keep track of the files in the reserve pool.
     */
    // TODO: 2018/3/22 by zmyer
    private static class ReserveFileMap {
        //保留文件集合
        private final ConcurrentMap<Long, Queue<File>> internalMap = new ConcurrentHashMap<>();

        /**
         * Add a file of the specified file size to the reserve file map.
         * @param sizeInBytes the size of the reserve file
         * @param reserveFile the reserve file to add.
         */
        // TODO: 2018/3/22 by zmyer
        void add(long sizeInBytes, File reserveFile) {
            internalMap.computeIfAbsent(sizeInBytes, key -> new ConcurrentLinkedQueue<>()).add(reserveFile);
        }

        /**
         * Remove and return a file of the specified size from the reserve file map.
         * @param sizeInBytes the size of the file to look for.
         * @return the {@link File}, or {@code null} if no file exists in the map for the specified size.
         */
        // TODO: 2018/3/22 by zmyer
        File remove(long sizeInBytes) {
            File reserveFile = null;
            //根据文件大小，获取具体的文件列表
            Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
            if (reserveFilesForSize != null && reserveFilesForSize.size() != 0) {
                //从保留的文件列表中获取一个文件对象
                reserveFile = reserveFilesForSize.remove();
            }
            //返回结果
            return reserveFile;
        }

        /**
         * @param sizeInBytes the size of files of interest
         * @return the number of files in the map of size {@code sizeInBytes}
         */
        // TODO: 2018/3/22 by zmyer
        int getCount(long sizeInBytes) {
            //获取指定大小的保留文件队列
            Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
            //获取文件队列大小
            return reserveFilesForSize != null ? reserveFilesForSize.size() : 0;
        }

        /**
         * @return the set of file sizes present in the map.
         */
        // TODO: 2018/3/22 by zmyer
        Set<Long> getFileSizeSet() {
            return internalMap.keySet();
        }
    }

    /**
     * Represents the state of pool initialization.
     */
    // TODO: 2018/3/22 by zmyer
    private enum PoolState {
        /**
         * If the pool is not yet created/inventoried or failed initialization.
         */
        NOT_INVENTORIED,

        /**
         * If the pool has been inventoried but not initialized to match new {@link DiskSpaceRequirements}.
         */
        INVENTORIED,

        /**
         * If the pool was successfully initialized.
         */
        INITIALIZED
    }
}
