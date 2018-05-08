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
package com.github.ambry.server;

import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request handler class
 */
// TODO: 2018/3/20 by zmyer
public class RequestHandler implements Runnable {
    private final int id;
    private final RequestResponseChannel requestChannel;
    private final AmbryRequests requests;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public RequestHandler(int id, RequestResponseChannel requestChannel, AmbryRequests requests) {
        this.id = id;
        this.requestChannel = requestChannel;
        this.requests = requests;
    }

    // TODO: 2018/3/20 by zmyer
    public void run() {
        while (true) {
            try {
                Request req = requestChannel.receiveRequest();
                if (req.equals(EmptyRequest.getInstance())) {
                    logger.debug("Request handler {} received shut down command", id);
                    return;
                }
                //开始处理请求对象
                requests.handleRequests(req);
                logger.trace("Request handler {} handling request {}", id, req);
            } catch (Throwable e) {
                // TODO add metric to track background threads
                logger.error("Exception when handling request", e);
                // this is bad and we need to shutdown the app
                Runtime.getRuntime().halt(1);
            }
        }
    }

    public void shutdown() throws InterruptedException {
        requestChannel.sendRequest(EmptyRequest.getInstance());
    }
}

// Request handler pool. A pool of threads that handle requests
// TODO: 2018/3/20 by zmyer
class RequestHandlerPool {
    //线程集合
    private Thread[] threads = null;
    //请求处理集合
    private RequestHandler[] handlers = null;
    //日志对象
    private Logger logger = LoggerFactory.getLogger(getClass());

    // TODO: 2018/3/20 by zmyer
    public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, AmbryRequests requests) {
        //创建线程集合
        threads = new Thread[numThreads];
        //创建请求处理集合
        handlers = new RequestHandler[numThreads];
        for (int i = 0; i < numThreads; i++) {
            //创建请求处理器
            handlers[i] = new RequestHandler(i, requestResponseChannel, requests);
            //创建处理线程
            threads[i] = Utils.daemonThread("request-handler-" + i, handlers[i]);
            //启动线程
            threads[i].start();
        }
    }

    // TODO: 2018/4/20 by zmyer
    public void shutdown() {
        try {
            logger.info("shutting down");
            for (RequestHandler handler : handlers) {
                //关闭请求处理器
                handler.shutdown();
            }
            for (Thread thread : threads) {
                //关闭线程
                thread.join();
            }
            logger.info("shut down completely");
        } catch (Exception e) {
            logger.error("error when shutting down request handler pool {}", e);
        }
    }
}
