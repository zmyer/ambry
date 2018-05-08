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
package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


// TODO: 2018/4/20 by zmyer
// The request at the network layer
class SocketServerRequest implements Request {
    //处理器
    private final int processor;
    //连接id
    private final String connectionId;
    //输入流对象
    private final InputStream input;
    //开始时间戳
    private final long startTimeInMs;
    //日志对象
    private Logger logger = LoggerFactory.getLogger(getClass());

    // TODO: 2018/4/20 by zmyer
    public SocketServerRequest(int processor, String connectionId, InputStream input) throws IOException {
        this.processor = processor;
        this.connectionId = connectionId;
        this.input = input;
        this.startTimeInMs = SystemTime.getInstance().milliseconds();
        logger.trace("Processor {} received request : {}", processor, connectionId);
    }

    @Override
    public InputStream getInputStream() {
        return input;
    }

    @Override
    public long getStartTimeInMs() {
        return startTimeInMs;
    }

    public int getProcessor() {
        return processor;
    }

    public String getConnectionId() {
        return connectionId;
    }
}

// The response at the network layer
// TODO: 2018/4/20 by zmyer
class SocketServerResponse implements Response {
    //处理器
    private final int processor;
    //请求对象
    private final Request request;
    //发送缓冲区
    private final Send output;
    //服务器网络应答统计对象
    private final ServerNetworkResponseMetrics metrics;
    //应答进入队列时间戳
    private long startQueueTimeInMs;

    // TODO: 2018/4/20 by zmyer
    public SocketServerResponse(Request request, Send output, ServerNetworkResponseMetrics metrics) {
        this.request = request;
        this.output = output;
        this.processor = ((SocketServerRequest) request).getProcessor();
        this.metrics = metrics;
    }

    public Send getPayload() {
        return output;
    }

    public Request getRequest() {
        return request;
    }

    public int getProcessor() {
        return processor;
    }

    // TODO: 2018/4/20 by zmyer
    public void onEnqueueIntoResponseQueue() {
        this.startQueueTimeInMs = SystemTime.getInstance().milliseconds();
    }

    // TODO: 2018/4/20 by zmyer
    public void onDequeueFromResponseQueue() {
        if (metrics != null) {
            metrics.updateQueueTime(SystemTime.getInstance().milliseconds() - startQueueTimeInMs);
        }
    }

    public NetworkSendMetrics getMetrics() {
        return metrics;
    }
}

// TODO: 2018/3/20 by zmyer
interface ResponseListener {
    public void onResponse(int processorId);
}

/**
 * RequestResponse channel for socket server
 */
// TODO: 2018/3/20 by zmyer
public class SocketRequestResponseChannel implements RequestResponseChannel {
    //处理器数量
    private final int numProcessors;
    //队列长度
    private final int queueSize;
    //请求队列
    private final ArrayBlockingQueue<Request> requestQueue;
    //应答队列集合
    private final ArrayList<BlockingQueue<Response>> responseQueues;
    //应答监听器集合
    private final ArrayList<ResponseListener> responseListeners;

    // TODO: 2018/3/20 by zmyer
    public SocketRequestResponseChannel(int numProcessors, int queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;
        this.requestQueue = new ArrayBlockingQueue<Request>(this.queueSize);
        responseQueues = new ArrayList<BlockingQueue<Response>>(this.numProcessors);
        responseListeners = new ArrayList<ResponseListener>();

        for (int i = 0; i < this.numProcessors; i++) {
            responseQueues.add(i, new LinkedBlockingQueue<Response>());
        }
    }

    /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
    // TODO: 2018/4/20 by zmyer
    @Override
    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    /** Send a response back to the socket server to be sent over the network */
    // TODO: 2018/4/20 by zmyer
    @Override
    public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics)
            throws InterruptedException {
        //创建请求应答对象
        SocketServerResponse response = new SocketServerResponse(originalRequest, payloadToSend, metrics);
        //应答进入应答队列
        response.onEnqueueIntoResponseQueue();
        //将应答消息插入到具体的处理器应答队列中
        responseQueues.get(response.getProcessor()).put(response);
        for (ResponseListener listener : responseListeners) {
            //开始触发应答事件
            listener.onResponse(response.getProcessor());
        }
    }

    /**
     * Closes the connection and does not send any response
     */
    // TODO: 2018/4/20 by zmyer
    @Override
    public void closeConnection(Request originalRequest) throws InterruptedException {
        //创建应答消息
        SocketServerResponse response = new SocketServerResponse(originalRequest, null, null);
        //插入应答队列
        responseQueues.get(response.getProcessor()).put(response);
        for (ResponseListener listener : responseListeners) {
            //开始应答处理
            listener.onResponse(response.getProcessor());
        }
    }

    /** Get the next request or block until there is one */
    // TODO: 2018/4/20 by zmyer
    @Override
    public Request receiveRequest() throws InterruptedException {
        return requestQueue.take();
    }

    /** Get a response for the given processor if there is one */
    // TODO: 2018/4/20 by zmyer
    public Response receiveResponse(int processor) throws InterruptedException {
        //从应答队列中读取消息
        return responseQueues.get(processor).poll();
    }

    // TODO: 2018/4/20 by zmyer
    public void addResponseListener(ResponseListener listener) {
        responseListeners.add(listener);
    }

    // TODO: 2018/4/20 by zmyer
    public int getRequestQueueSize() {
        return requestQueue.size();
    }

    // TODO: 2018/4/20 by zmyer
    public int getResponseQueueSize(int processor) {
        return responseQueues.get(processor).size();
    }

    // TODO: 2018/4/20 by zmyer
    public int getNumberOfProcessors() {
        return numProcessors;
    }

    // TODO: 2018/4/20 by zmyer
    public void shutdown() {
        requestQueue.clear();
    }
}


