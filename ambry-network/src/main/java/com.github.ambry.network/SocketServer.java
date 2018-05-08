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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
// TODO: 2018/3/19 by zmyer
public class SocketServer implements NetworkServer {
    //主机名
    private final String host;
    //端口
    private final int port;
    //处理线程数目
    private final int numProcessorThreads;
    //最大请求队列长度
    private final int maxQueuedRequests;
    //发送缓存大小
    private final int sendBufferSize;
    //接受缓冲区大小
    private final int recvBufferSize;
    //最大请求大小
    private final int maxRequestSize;
    //处理器列表
    private final ArrayList<Processor> processors;
    //接收器列表
    private volatile ArrayList<Acceptor> acceptors;
    //请求应答通道对象
    private final SocketRequestResponseChannel requestResponseChannel;
    //日志对象
    private Logger logger = LoggerFactory.getLogger(getClass());
    //服务器日志统计
    private final ServerNetworkMetrics metrics;
    //端口集合
    private final HashMap<PortType, Port> ports;
    //SSL工厂对象
    private SSLFactory sslFactory;

    // TODO: 2018/3/20 by zmyer
    public SocketServer(NetworkConfig config, SSLConfig sslConfig, MetricRegistry registry, ArrayList<Port> portList) {
        this.host = config.hostName;
        this.port = config.port;
        this.numProcessorThreads = config.numIoThreads;
        this.maxQueuedRequests = config.queuedMaxRequests;
        this.sendBufferSize = config.socketSendBufferBytes;
        this.recvBufferSize = config.socketReceiveBufferBytes;
        this.maxRequestSize = config.socketRequestMaxBytes;
        processors = new ArrayList<Processor>(numProcessorThreads);
        requestResponseChannel = new SocketRequestResponseChannel(numProcessorThreads, maxQueuedRequests);
        metrics = new ServerNetworkMetrics(requestResponseChannel, registry, processors);
        this.acceptors = new ArrayList<Acceptor>();
        this.ports = new HashMap<PortType, Port>();
        this.validatePorts(portList);
        this.initializeSSLFactory(sslConfig);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getSSLPort() {
        Port sslPort = ports.get(PortType.SSL);
        if (sslPort != null) {
            return sslPort.getPort();
        }
        throw new IllegalStateException("No SSL Port Exists for Server " + host + ":" + port);
    }

    // TODO: 2018/4/20 by zmyer
    private void initializeSSLFactory(SSLConfig sslConfig) {
        if (ports.get(PortType.SSL) != null) {
            try {
                //创建SSL工厂对象
                this.sslFactory = new SSLFactory(sslConfig);
                metrics.sslFactoryInitializationCount.inc();
            } catch (Exception e) {
                metrics.sslFactoryInitializationErrorCount.inc();
                throw new IllegalStateException("Exception thrown during initialization of SSLFactory ", e);
            }
        }
    }

    public int getNumProcessorThreads() {
        return numProcessorThreads;
    }

    public int getMaxQueuedRequests() {
        return maxQueuedRequests;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public int getRecvBufferSize() {
        return recvBufferSize;
    }

    public int getMaxRequestSize() {
        return maxRequestSize;
    }

    @Override
    public RequestResponseChannel getRequestResponseChannel() {
        return requestResponseChannel;
    }

    // TODO: 2018/4/20 by zmyer
    private void validatePorts(ArrayList<Port> portList) {
        HashSet<PortType> portTypeSet = new HashSet<PortType>();
        for (Port port : portList) {
            if (portTypeSet.contains(port.getPortType())) {
                throw new IllegalArgumentException(
                        "Not more than one port of same type is allowed : " + port.getPortType());
            } else {
                portTypeSet.add(port.getPortType());
                this.ports.put(port.getPortType(), port);
            }
        }
    }

    // TODO: 2018/3/19 by zmyer
    public void start() throws IOException, InterruptedException {
        logger.info("Starting {} processor threads", numProcessorThreads);
        for (int i = 0; i < numProcessorThreads; i++) {
            //创建processor对象
            processors.add(i, new Processor(i, maxRequestSize, requestResponseChannel, metrics, sslFactory));
            //启动processor线程
            Utils.newThread("ambry-processor-" + port + " " + i, processors.get(i), false).start();
        }

        //在channel对象上注册应答监听器
        requestResponseChannel.addResponseListener(new ResponseListener() {
            @Override
            public void onResponse(int processorId) {
                //如果有消息到来，使用指定的processor线程处理
                processors.get(processorId).wakeup();
            }
        });

        // start accepting connections
        logger.info("Starting acceptor threads");
        //创建accept对象，负责监听接受新连接
        Acceptor plainTextAcceptor = new Acceptor(port, processors, sendBufferSize, recvBufferSize, metrics);
        this.acceptors.add(plainTextAcceptor);
        //启动accept线程
        Utils.newThread("ambry-acceptor", plainTextAcceptor, false).start();

        //如果开启了SSL，则需要启动accept SSL线程
        Port sslPort = ports.get(PortType.SSL);
        if (sslPort != null) {
            SSLAcceptor sslAcceptor = new SSLAcceptor(sslPort.getPort(), processors, sendBufferSize, recvBufferSize,
                    metrics);
            acceptors.add(sslAcceptor);
            Utils.newThread("ambry-sslacceptor", sslAcceptor, false).start();
        }
        //等待Accept线程启动完毕
        for (Acceptor acceptor : acceptors) {
            acceptor.awaitStartup();
        }
        logger.info("Started server");
    }

    // TODO: 2018/4/20 by zmyer
    public void shutdown() {
        try {
            logger.info("Shutting down server");
            for (Acceptor acceptor : acceptors) {
                if (acceptor != null) {
                    //关闭接收器
                    acceptor.shutdown();
                }
            }
            for (Processor processor : processors) {
                //关闭所有的处理器
                processor.shutdown();
            }
            logger.info("Shutdown completed");
        } catch (Exception e) {
            logger.error("Error shutting down socket server {}", e);
        }
    }
}

/**
 * A base class with some helper variables and methods
 */
// TODO: 2018/3/19 by zmyer
abstract class AbstractServerThread implements Runnable {
    //启动屏障对象
    private final CountDownLatch startupLatch;
    //关闭屏障对象
    private final CountDownLatch shutdownLatch;
    //是否存活标记
    private final AtomicBoolean alive;
    //日志对象
    protected Logger logger = LoggerFactory.getLogger(getClass());

    // TODO: 2018/4/20 by zmyer
    public AbstractServerThread() throws IOException {
        startupLatch = new CountDownLatch(1);
        shutdownLatch = new CountDownLatch(1);
        alive = new AtomicBoolean(false);
    }

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    // TODO: 2018/4/20 by zmyer
    public void shutdown() throws InterruptedException {
        alive.set(false);
        shutdownLatch.await();
    }

    /**
     * Wait for the thread to completely start up
     */
    // TODO: 2018/3/20 by zmyer
    public void awaitStartup() throws InterruptedException {
        startupLatch.await();
    }

    /**
     * Record that the thread startup is complete
     */
    // TODO: 2018/4/20 by zmyer
    protected void startupComplete() {
        alive.set(true);
        startupLatch.countDown();
    }

    /**
     * Record that the thread shutdown is complete
     */
    // TODO: 2018/4/20 by zmyer
    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    /**
     * Is the server still running?
     */
    // TODO: 2018/4/20 by zmyer
    protected boolean isRunning() {
        return alive.get();
    }
}

/**
 * Thread that accepts and configures new connections.
 */
// TODO: 2018/3/20 by zmyer
class Acceptor extends AbstractServerThread {
    //处理器集合
    private final ArrayList<Processor> processors;
    //发送缓冲区大小
    private final int sendBufferSize;
    //接受缓冲区大小
    private final int recvBufferSize;
    //服务器socket通道对象
    private final ServerSocketChannel serverChannel;
    //Selector对象主要负责接收新连接
    private final java.nio.channels.Selector nioSelector;
    //连接超时时间
    private static final long selectTimeOutMs = 500;
    //服务器网络日志统计
    private final ServerNetworkMetrics metrics;
    //日志对象
    protected Logger logger = LoggerFactory.getLogger(getClass());

    // TODO: 2018/4/20 by zmyer
    public Acceptor(int port, ArrayList<Processor> processors, int sendBufferSize, int recvBufferSize,
            ServerNetworkMetrics metrics) throws IOException {
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.serverChannel = openServerSocket(port);
        this.nioSelector = java.nio.channels.Selector.open();
        this.metrics = metrics;
    }

    /**
     * Accept loop that checks for new connection attempts for a plain text port
     */
    // TODO: 2018/3/20 by zmyer
    public void run() {
        try {
            //将当前的服务器socket通道对象注册到选择器中，来接受新连接
            serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
            //启动完成
            startupComplete();
            int currentProcessor = 0;
            while (isRunning()) {
                //开始进行轮询
                int ready = nioSelector.select(selectTimeOutMs);
                if (ready > 0) {
                    Set<SelectionKey> keys = nioSelector.selectedKeys();
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext() && isRunning()) {
                        SelectionKey key = null;
                        try {
                            key = iter.next();
                            iter.remove();
                            if (key.isAcceptable()) {
                                //开始接受新连接，并将其转交给指定的processor对象进行处理
                                accept(key, processors.get(currentProcessor));
                            } else {
                                throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                            }

                            // round robin to the next processor thread
                            //递增当前处理器索引位置
                            currentProcessor = (currentProcessor + 1) % processors.size();
                        } catch (Exception e) {
                            key.cancel();
                            metrics.acceptConnectionErrorCount.inc();
                            logger.debug("Error in accepting new connection", e);
                        }
                    }
                }
            }
            logger.debug("Closing server socket and selector.");
            //关闭接收新连接的服务器端通道对象
            serverChannel.close();
            //关闭选择器
            nioSelector.close();
            //关闭完成
            shutdownComplete();
            super.shutdown();
        } catch (Exception e) {
            metrics.acceptorShutDownErrorCount.inc();
            logger.error("Error during shutdown of acceptor thread", e);
        }
    }

    /*
     * Create a server socket to listen for connections on.
     */
    // TODO: 2018/4/20 by zmyer
    private ServerSocketChannel openServerSocket(int port) throws IOException {
        //创建服务器通道对象
        InetSocketAddress address = new InetSocketAddress(port);
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        //绑定服务器端地址
        serverChannel.socket().bind(address);
        logger.info("Awaiting socket connections on {}:{}", address.getHostName(), port);
        return serverChannel;
    }

    /*
     * Accept a new connection
     */
    // TODO: 2018/3/20 by zmyer
    protected void accept(SelectionKey key, Processor processor) throws SocketException, IOException {
        //接收新连接
        SocketChannel socketChannel = acceptConnection(key);
        //processor接收新连接
        processor.accept(socketChannel, PortType.PLAINTEXT);
    }

    // TODO: 2018/3/20 by zmyer
    protected SocketChannel acceptConnection(SelectionKey key) throws SocketException, IOException {
        //获取当前服务器socket通道对象
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        //设置接受缓冲区大小
        serverSocketChannel.socket().setReceiveBufferSize(recvBufferSize);
        //获取当前服务器socket对象对应的客户端通道对象
        SocketChannel socketChannel = serverSocketChannel.accept();
        //设置非阻塞模式
        socketChannel.configureBlocking(false);
        //设置非延时
        socketChannel.socket().setTcpNoDelay(true);
        //设置发送缓冲区大小
        socketChannel.socket().setSendBufferSize(sendBufferSize);
        logger.trace("Accepted connection from {} on {}. sendBufferSize "
                        + "[actual|requested]: [{}|{}] recvBufferSize [actual|requested]: [{}|{}]",
                socketChannel.socket().getInetAddress(), socketChannel.socket().getLocalSocketAddress(),
                socketChannel.socket().getSendBufferSize(), sendBufferSize,
                socketChannel.socket().getReceiveBufferSize(),
                recvBufferSize);
        //返回通道对象
        return socketChannel;
    }

    // TODO: 2018/4/20 by zmyer
    public void shutdown() throws InterruptedException {
        nioSelector.wakeup();
        super.shutdown();
    }
}

/**
 * Thread that accepts and configures new connections for an SSL Port
 */
// TODO: 2018/3/20 by zmyer
class SSLAcceptor extends Acceptor {

    // TODO: 2018/4/20 by zmyer
    public SSLAcceptor(int port, ArrayList<Processor> processors, int sendBufferSize, int recvBufferSize,
            ServerNetworkMetrics metrics) throws IOException {
        super(port, processors, sendBufferSize, recvBufferSize, metrics);
    }

    /*
     * Accept a new connection
     */
    // TODO: 2018/4/20 by zmyer
    @Override
    protected void accept(SelectionKey key, Processor processor) throws SocketException, IOException {
        SocketChannel socketChannel = acceptConnection(key);
        processor.accept(socketChannel, PortType.SSL);
    }
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
// TODO: 2018/3/19 by zmyer
class Processor extends AbstractServerThread {
    //请求最大长度
    private final int maxRequestSize;
    //请求应答通道对象
    private final SocketRequestResponseChannel channel;
    //处理器id
    private final int id;
    //计时器
    private final Time time;
    //连接集合
    private final ConcurrentLinkedQueue<SocketChannelPortTypePair> newConnections =
            new ConcurrentLinkedQueue<SocketChannelPortTypePair>();
    //选择器
    private final Selector selector;
    //服务器网络统计对象
    private final ServerNetworkMetrics metrics;
    //轮询超时时间
    private static final long pollTimeoutMs = 300;

    // TODO: 2018/4/20 by zmyer
    Processor(int id, int maxRequestSize, RequestResponseChannel channel, ServerNetworkMetrics metrics,
            SSLFactory sslFactory) throws IOException {
        this.maxRequestSize = maxRequestSize;
        this.channel = (SocketRequestResponseChannel) channel;
        this.id = id;
        this.time = SystemTime.getInstance();
        selector = new Selector(metrics, time, sslFactory);
        this.metrics = metrics;
    }

    // TODO: 2018/4/20 by zmyer
    public void run() {
        try {
            //启动完成
            startupComplete();
            while (isRunning()) {
                // setup any new connections that have been queued up
                //配置新的连接
                configureNewConnections();
                // register any new responses for writing
                //开始处理新的应答消息
                processNewResponses();
                //开始在选择器上进行轮询
                selector.poll(pollTimeoutMs);

                // handle completed receives
                //从选择器中读取接收完成的应答消息
                List<NetworkReceive> completedReceives = selector.completedReceives();
                for (NetworkReceive networkReceive : completedReceives) {
                    //读取连接id
                    String connectionId = networkReceive.getConnectionId();
                    //创建请求对象
                    SocketServerRequest req = new SocketServerRequest(id, connectionId,
                            new ByteBufferInputStream(networkReceive.getReceivedBytes().getPayload()));
                    //发送请求对象
                    channel.sendRequest(req);
                }
            }
        } catch (Exception e) {
            logger.error("Error in processor thread", e);
        } finally {
            logger.debug("Closing server socket and selector.");
            try {
                //关闭选择器
                closeAll();
                //关闭完成
                shutdownComplete();
                super.shutdown();
            } catch (InterruptedException ie) {
                metrics.processorShutDownErrorCount.inc();
                logger.error("InterruptedException on processor shutdown ", ie);
            }
        }
    }

    // TODO: 2018/4/20 by zmyer
    private void processNewResponses() throws InterruptedException, IOException {
        //根据提供的processor获取通道对象中待处理的应答消息
        SocketServerResponse curr = (SocketServerResponse) channel.receiveResponse(id);
        while (curr != null) {
            //更新应答消息出队列时间戳
            curr.onDequeueFromResponseQueue();
            //读取请求消息
            SocketServerRequest request = (SocketServerRequest) curr.getRequest();
            //读取请求消息对应的连接id
            String connectionId = request.getConnectionId();
            try {
                if (curr.getPayload() == null) {
                    // We should never need to send an empty response. If the payload is empty, we will assume error
                    // and close the connection
                    logger.trace("Socket server received no response and hence closing the connection");
                    //如果当前的请求中没有具体的数据，则直接关闭连接
                    selector.close(connectionId);
                } else {
                    logger.trace("Socket server received response to send, registering for write: {}", curr);
                    //创建发送缓冲区
                    NetworkSend networkSend = new NetworkSend(connectionId, curr.getPayload(), curr.getMetrics(), time);
                    //开始发送应答消息
                    selector.send(networkSend);
                }
            } catch (IllegalStateException e) {
                metrics.processNewResponseErrorCount.inc();
                logger.debug("Error in processing new responses", e);
            } finally {
                //读取下一条待发送的应答消息
                curr = (SocketServerResponse) channel.receiveResponse(id);
            }
        }
    }

    /**
     * Queue up a new connection for reading
     */
    // TODO: 2018/3/20 by zmyer
    public void accept(SocketChannel socketChannel, PortType portType) {
        //将新建的连接插入到集合中
        newConnections.add(new SocketChannelPortTypePair(socketChannel, portType));
        //唤醒阻塞的选择器对象
        wakeup();
    }

    /**
     * Close all open connections
     */
    // TODO: 2018/4/20 by zmyer
    private void closeAll() {
        //关闭选择器
        selector.close();
    }

    /**
     * Register any new connections that have been queued up
     */
    // TODO: 2018/3/20 by zmyer
    private void configureNewConnections() throws ClosedChannelException, IOException {
        while (newConnections.size() > 0) {
            //从连接队列中读取新建的连接对象
            SocketChannelPortTypePair socketChannelPortTypePair = newConnections.poll();
            logger.debug("Processor {} listening to new connection from {}", id,
                    socketChannelPortTypePair.getSocketChannel().socket().getRemoteSocketAddress());
            try {
                //将新的连接对象注册到选择器中
                selector.register(socketChannelPortTypePair.getSocketChannel(),
                        socketChannelPortTypePair.getPortType());
            } catch (IOException e) {
                logger.error("Error on registering new connection ", e);
            }
        }
    }

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    // TODO: 2018/4/20 by zmyer
    public void shutdown() throws InterruptedException {
        selector.wakeup();
        super.shutdown();
    }

    /**
     * Wakes up the thread for selection.
     */
    // TODO: 2018/4/20 by zmyer
    public void wakeup() {
        selector.wakeup();
    }

    // TODO: 2018/4/20 by zmyer
    class SocketChannelPortTypePair {
        private SocketChannel socketChannel;
        private PortType portType;

        public SocketChannelPortTypePair(SocketChannel socketChannel, PortType portType) {
            this.socketChannel = socketChannel;
            this.portType = portType;
        }

        public PortType getPortType() {
            return portType;
        }

        public SocketChannel getSocketChannel() {
            return this.socketChannel;
        }
    }
}
