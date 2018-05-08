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

import com.github.ambry.utils.Throttler;

import java.util.HashMap;
import java.util.Map;


/**
 * Helps schedule I/O operations.
 * This is meant to be used by
 * 1. Application reads/writes from/to the log.
 * 2. Hard delete
 * 3. Compaction
 * The initial implementation simply returns MAX_VALUE for the application and uses the throttler for the other 2.
 * In the future this will have functions to submit feedback so that more intelligent decisions can be made.
 */
// TODO: 2018/3/22 by zmyer
class DiskIOScheduler {
    //节流器集合
    private final Map<String, Throttler> throttlers;

    /**
     * Create a {@link DiskIOScheduler}.
     * @param throttlers the {@link Throttler}s to use for each job type.
     */
    // TODO: 2018/3/22 by zmyer
    DiskIOScheduler(Map<String, Throttler> throttlers) {
        this.throttlers = throttlers != null ? throttlers : new HashMap<String, Throttler>();
    }

    /**
     * Return the size of I/O permissible based on the parameters provided.
     * @param jobType the type of the job requesting an I/O slice.
     * @param jobId the ID of the job requesting an I/O slice.
     * @param usedSinceLastCall the amount of capacity used since the last call to this function.
     * @return the I/O slice available for use.
     */
    // TODO: 2018/3/22 by zmyer
    long getSlice(String jobType, String jobId, long usedSinceLastCall) {
        //根据任务类型，获取对应的节流器
        Throttler throttler = throttlers.get(jobType);
        if (throttler != null) {
            try {
                //检查节流器是否需要立即节流
                throttler.maybeThrottle(usedSinceLastCall);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Throttler call interrupted", e);
            }
        }
        //返回结果
        return Long.MAX_VALUE;
    }

    /**
     * Disables the DiskIOScheduler i.e. there will be no more blocking calls
     */
    // TODO: 2018/3/22 by zmyer
    void disable() {
        for (Throttler throttler : throttlers.values()) {
            throttler.disable();
        }
    }
}

