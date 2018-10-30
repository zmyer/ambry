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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A selector doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit network requests and responses.
 * <p>
 * A connection can be added to the selector by doing
 *
 * <pre>
 * selector.connect("connectionId", new InetSocketAddress(&quot;linkedin.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established. The
 * call on return provides a unique id that identifies this connection
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * List&lt;NetworkSend&gt; requestsToSend = Arrays.asList(new NetworkSend(0, bytes), new NetworkSend(1, otherBytes));
 * selector.poll(TIMEOUT_MS, requestsToSend);
 * </pre>
 *
 * The selector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 */
// TODO: 2018/3/20 by zmyer
public class Selector implements Selectable {
    //日志对象
    private static final Logger logger = LoggerFactory.getLogger(Selector.class);

    //selector对象
    private final java.nio.channels.Selector nioSelector;
    //key集合
    private final Map<String, SelectionKey> keyMap;
    //完成发送消息集合
    private final List<NetworkSend> completedSends;
    //完成接受消息集合
    private final List<NetworkReceive> completedReceives;
    //断开的连接
    private final List<String> disconnected;
    //关闭的连接
    private final List<String> closedConnections;
    //已建立的连接
    private final List<String> connected;
    //未准备就绪的连接
    private final Set<String> unreadyConnections;
    //计时器
    private final Time time;
    private final NetworkMetrics metrics;
    private final AtomicLong IdGenerator;
    //活跃的连接数
    private final AtomicLong numActiveConnections;
    //SSL工厂
    private final SSLFactory sslFactory;

    /**
     * Create a new selector
     */
    // TODO: 2018/3/27 by zmyer
    public Selector(NetworkMetrics metrics, Time time, SSLFactory sslFactory) throws IOException {
        this.nioSelector = java.nio.channels.Selector.open();
        this.time = time;
        this.keyMap = new HashMap<String, SelectionKey>();
        this.completedSends = new ArrayList<NetworkSend>();
        this.completedReceives = new ArrayList<NetworkReceive>();
        this.connected = new ArrayList<String>();
        this.disconnected = new ArrayList<String>();
        this.closedConnections = new ArrayList<>();
        this.metrics = metrics;
        this.IdGenerator = new AtomicLong(0);
        numActiveConnections = new AtomicLong(0);
        unreadyConnections = new HashSet<>();
        metrics.registerSelectorActiveConnections(numActiveConnections);
        this.sslFactory = sslFactory;
    }

    /**
     * Generate an unique connection id
     * @param channel The channel between two hosts
     * @return The id for the connection that was created
     */
    // TODO: 2018/3/27 by zmyer
    private String generateConnectionId(SocketChannel channel) {
        Socket socket = channel.socket();
        String localHost = socket.getLocalAddress().getHostAddress();
        int localPort = socket.getLocalPort();
        String remoteHost = socket.getInetAddress().getHostAddress();
        int remotePort = socket.getPort();
        long connectionIdSuffix = IdGenerator.getAndIncrement();
        StringBuilder connectionIdBuilder = new StringBuilder();
        connectionIdBuilder.append(localHost)
                .append(":")
                .append(localPort)
                .append("-")
                .append(remoteHost)
                .append(":")
                .append(remotePort)
                .append("_")
                .append(connectionIdSuffix);
        return connectionIdBuilder.toString();
    }

    /**
     * Begin connecting to the given address and add the connection to this selector and returns an id that identifies
     * the connection
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param address The address to connect to
     * @param sendBufferSize The networkSend buffer size for the new connection
     * @param receiveBufferSize The receive buffer size for the new connection
     * @param portType {@PortType} which represents the type of connection to establish
     * @return The id for the connection that was created
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the server is down
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType)
            throws IOException {
        //创建channel对象
        SocketChannel channel = SocketChannel.open();
        //设置非阻塞模式
        channel.configureBlocking(false);
        //读取socket
        Socket socket = channel.socket();
        socket.setKeepAlive(true);
        //设置socket发送缓冲区大小
        socket.setSendBufferSize(sendBufferSize);
        //设置socket接受缓冲区大小
        socket.setReceiveBufferSize(receiveBufferSize);
        socket.setTcpNoDelay(true);
        try {
            //开始连接
            channel.connect(address);
        } catch (UnresolvedAddressException e) {
            //连接关闭
            channel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            channel.close();
            throw e;
        }
        //创建当前连接对应的连接id
        String connectionId = generateConnectionId(channel);
        //开始将channel注册到selector对象中
        SelectionKey key = channel.register(this.nioSelector, SelectionKey.OP_CONNECT);
        Transmission transmission = null;
        try {
            //创建传输对象
            transmission =
                    TransmissionFactory.getTransmission(connectionId, channel, key, address.getHostName(),
                            address.getPort(),
                            time, metrics, portType, sslFactory, SSLFactory.Mode.CLIENT);
        } catch (IOException e) {
            logger.error("IOException on transmission creation " + e);
            channel.socket().close();
            channel.close();
            throw e;
        }
        //将传输对象注册到key中
        key.attach(transmission);
        //保存key与连接id之间的关系
        this.keyMap.put(connectionId, key);
        //设置目前已经完成连接的数目
        numActiveConnections.set(this.keyMap.size());
        return connectionId;
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     */
    // TODO: 2018/3/20 by zmyer
    public String register(SocketChannel channel, PortType portType) throws IOException {
        //获取socket对象
        Socket socket = channel.socket();
        //生成连接id
        String connectionId = generateConnectionId(channel);
        //将channel对象注册到selector中
        SelectionKey key = channel.register(nioSelector, SelectionKey.OP_READ);
        Transmission transmission = null;
        try {
            //创建传输对象
            transmission =
                    TransmissionFactory.getTransmission(connectionId, channel, key,
                            socket.getInetAddress().getHostAddress(),
                            socket.getPort(), time, metrics, portType, sslFactory, SSLFactory.Mode.SERVER);
        } catch (IOException e) {
            logger.error("IOException on transmission creation " + e);
            socket.close();
            channel.close();
            throw e;
        }
        //将传输对象注册到key中
        key.attach(transmission);
        //将key插入到集合中
        this.keyMap.put(connectionId, key);
        //更新连接数
        numActiveConnections.set(this.keyMap.size());
        return connectionId;
    }

    /**
     * Disconnect any connections for the given id (if there are any). The disconnection is asynchronous and will not be
     * processed until the next {@link #poll(long) poll()} call.
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void disconnect(String connectionId) {
        SelectionKey key = this.keyMap.get(connectionId);
        if (key != null) {
            key.cancel();
        }
    }

    /**
     * Interrupt the selector if it is blocked waiting to do I/O.
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void wakeup() {
        nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void close() {
        for (SelectionKey key : this.nioSelector.keys()) {
            //关闭所有的key
            close(key);
        }
        try {
            //关闭selector对象
            this.nioSelector.close();
        } catch (IOException e) {
            metrics.selectorNioCloseErrorCount.inc();
            logger.error("Exception closing nioSelector:", e);
        }
    }

    /**
     * Tells whether or not this selector is open.  </p>
     *
     * @return <tt>true</tt> if, and only if, this selector is open
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public boolean isOpen() {
        return nioSelector.isOpen();
    }

    /**
     * Queue the given request for sending in the subsequent {@poll(long)} calls
     * @param networkSend The NetworkSend that is ready to be sent
     */
    // TODO: 2018/3/27 by zmyer
    public void send(NetworkSend networkSend) {
        //根据连接id，查找对应的key
        SelectionKey key = keyForId(networkSend.getConnectionId());
        if (key == null) {
            throw new IllegalStateException("Attempt to send data to a null key");
        }
        //从key中读取对应的传输对象
        Transmission transmission = getTransmission(key);
        try {
            //开始发送数据对象
            transmission.setNetworkSend(networkSend);
        } catch (CancelledKeyException e) {
            logger.debug("Ignoring response for closed socket.");
            close(key);
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     * <p>
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each {@link #poll(long)} call and repopulated by the call if any
     * completed I/O.
     *
     * @param timeoutMs The amount of time to wait, in milliseconds. If negative, wait indefinitely.
     *
     * @throws IOException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void poll(long timeoutMs) throws IOException {
        poll(timeoutMs, null);
    }

    /**
     * Firstly initiate the provided sends. Then do whatever I/O can be done on each connection without blocking.
     * This includes completing connections, completing disconnections, initiating new sends,
     * or making progress on in-progress sends or receives.
     * <p>
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each {@link #poll(long, List)} call and repopulated by the call if any
     * completed I/O.
     *
     * @param timeoutMs The amount of time to wait, in milliseconds. If negative, wait indefinitely.
     * @param sends The list of new sends to begin
     *
     * @throws IOException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void poll(long timeoutMs, List<NetworkSend> sends) throws IOException {
        clear();

        // register for write interest on any new sends
        if (sends != null) {
            for (NetworkSend networkSend : sends) {
                //开始发送消息
                send(networkSend);
            }
        }

    // check ready keys
    long startSelect = time.milliseconds();
    int readyKeys = select(timeoutMs);
    this.metrics.selectorSelectCount.inc();

    if (readyKeys > 0) {
      long endSelect = time.milliseconds();
      this.metrics.selectorSelectTime.update(endSelect - startSelect);
      Set<SelectionKey> keys = nioSelector.selectedKeys();
      Iterator<SelectionKey> iter = keys.iterator();
      while (iter.hasNext()) {
        SelectionKey key = iter.next();
        iter.remove();

                //从key中读取传输对象
                Transmission transmission = getTransmission(key);
                try {
                    if (key.isConnectable()) {
                        //连接正式建立
                        transmission.finishConnect();
                        if (transmission.ready()) {
                            //将已经建立的连接id插入到集合中
                            connected.add(transmission.getConnectionId());
                            metrics.selectorConnectionCreated.inc();
                        } else {
                            //将未完成的连接id插入到集合中
                            unreadyConnections.add(transmission.getConnectionId());
                        }
                    }

          /* if channel is not ready, finish prepare */
                    if (transmission.isConnected() && !transmission.ready()) {
                        transmission.prepare();
                        continue;
                    }

          if (key.isReadable() && transmission.ready()) {
            read(key, transmission);
          } else if (key.isWritable() && transmission.ready()) {
            write(key, transmission);
          } else if (!key.isValid()) {
            close(key);
          }
        } catch (IOException e) {
          String socketDescription = socketDescription(channel(key));
          if (e instanceof EOFException || e instanceof ConnectException) {
            metrics.selectorDisconnectedErrorCount.inc();
            logger.error("Connection {} disconnected", socketDescription, e);
          } else {
            metrics.selectorIOErrorCount.inc();
            logger.warn("Error in I/O with connection to {}", socketDescription, e);
          }
          close(key);
        } catch (Exception e) {
          metrics.selectorKeyOperationErrorCount.inc();
          logger.error("closing key on exception remote host {}", channel(key).socket().getRemoteSocketAddress(), e);
          close(key);
        }
      }
      checkUnreadyConnectionsStatus();
      this.metrics.selectorIOCount.inc();
      this.metrics.selectorIOTime.update(time.milliseconds() - endSelect);
    }
    disconnected.addAll(closedConnections);
    closedConnections.clear();
  }

    /**
     * Check readiness for unready connections and add to completed list if ready
     */
    // TODO: 2018/3/27 by zmyer
    private void checkUnreadyConnectionsStatus() {
        Iterator<String> iterator = unreadyConnections.iterator();
        while (iterator.hasNext()) {
            String connId = iterator.next();
            if (isChannelReady(connId)) {
                connected.add(connId);
                iterator.remove();
                metrics.selectorConnectionCreated.inc();
            }
        }
    }

    /**
     * Generate the description for a SocketChannel
     */
    // TODO: 2018/3/27 by zmyer
    private String socketDescription(SocketChannel channel) {
        Socket socket = channel.socket();
        if (socket == null) {
            return "[unconnected socket]";
        } else if (socket.getInetAddress() != null) {
            return socket.getInetAddress().toString();
        } else {
            return socket.getLocalAddress().toString();
        }
    }

    /**
     * Returns {@code true} if channel is ready to send or receive data, {@code false} otherwise
     * @param connectionId upon which readiness is checked for
     * @return true if channel is ready to accept reads/writes, false otherwise
     */
    // TODO: 2018/3/27 by zmyer
    public boolean isChannelReady(String connectionId) {
        Transmission transmission = getTransmission(keyForId(connectionId));
        return transmission.ready();
    }

    @Override
    public List<NetworkSend> completedSends() {
        return this.completedSends;
    }

    // TODO: 2018/4/20 by zmyer
    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    public long getNumActiveConnections() {
        return numActiveConnections.get();
    }

    /**
     * Clear the results from the prior poll
     */
    // TODO: 2018/3/27 by zmyer
    private void clear() {
        completedSends.clear();
        completedReceives.clear();
        connected.clear();
        disconnected.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
     * @return The number of keys ready
     * @throws IOException
     */
    // TODO: 2018/3/27 by zmyer
    private int select(long ms) throws IOException {
        if (ms == 0L) {
            return this.nioSelector.selectNow();
        } else if (ms < 0L) {
            return this.nioSelector.select();
        } else {
            return this.nioSelector.select(ms);
        }
    }

    /**
     * Begin closing this connection by given connection id
     */
    // TODO: 2018/3/27 by zmyer
    @Override
    public void close(String connectionId) {
        SelectionKey key = keyForId(connectionId);
        if (key == null) {
            metrics.selectorCloseKeyErrorCount.inc();
            logger.error("Attempt to close socket for which there is no open connection. Connection id {}",
                    connectionId);
        } else {
            close(key);
        }
    }

    /**
     * Begin closing this connection by given key
     */
    // TODO: 2018/3/27 by zmyer
    private void close(SelectionKey key) {
        //获取传输对象
        Transmission transmission = getTransmission(key);
        if (transmission != null) {
            logger.debug("Closing connection from {}", transmission.getConnectionId());
            closedConnections.add(transmission.getConnectionId());
            keyMap.remove(transmission.getConnectionId());
            numActiveConnections.set(keyMap.size());
            unreadyConnections.remove(transmission.getConnectionId());
            try {
                transmission.close();
            } catch (IOException e) {
                logger.error("IOException thrown during closing of transmission with connectionId {} :",
                        transmission.getConnectionId(), e);
            }
        } else {
            key.attach(null);
            key.cancel();
            SocketAddress address = null;
            try {
                SocketChannel socketChannel = channel(key);
                address = socketChannel.socket().getRemoteSocketAddress();
                socketChannel.socket().close();
                socketChannel.close();
            } catch (IOException e) {
                metrics.selectorCloseSocketErrorCount.inc();
                logger.error("Exception closing connection to remote host {} :", address, e);
            }
        }
        this.metrics.selectorConnectionClosed.inc();
    }

    /**
     * Get the selection key associated with this numeric id
     */
    // TODO: 2018/3/27 by zmyer
    private SelectionKey keyForId(String id) {
        return this.keyMap.get(id);
    }

    /**
     * Process reads from ready sockets
     */
    // TODO: 2018/3/27 by zmyer
    private void read(SelectionKey key, Transmission transmission) throws IOException {
        long startTimeToReadInMs = time.milliseconds();
        try {
            //开始读取
            boolean readComplete = transmission.read();
            if (readComplete) {
                //将读取完毕的消息插入到集合中
                this.completedReceives.add(transmission.getNetworkReceive());
                //触发接受完成操作
                transmission.onReceiveComplete();
                //清空
                transmission.clearReceive();
            }
        } finally {
            long readTime = time.milliseconds() - startTimeToReadInMs;
            logger.trace("SocketServer time spent on read per key {} = {}", transmission.getConnectionId(), readTime);
        }
    }

    /**
     * Process writes to ready sockets
     */
    // TODO: 2018/3/27 by zmyer
    private void write(SelectionKey key, Transmission transmission) throws IOException {
        long startTimeToWriteInMs = time.milliseconds();
        try {
            //开始发送
            boolean sendComplete = transmission.write();
            if (sendComplete) {
                logger.trace("Finished writing, registering for read on connection {}",
                        transmission.getRemoteSocketAddress());
                //触发发送完成操作
                transmission.onSendComplete();
                //将发送完成的消息插入到集合中
                this.completedSends.add(transmission.getNetworkSend());
                //递增发送过程中的消息数目
                metrics.sendInFlight.dec();
                //清空
                transmission.clearSend();
                //发送完毕，需要立即将写入事件关闭，并注册读事件，否则会阻塞读事件
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            }
        } finally {
            long writeTime = time.milliseconds() - startTimeToWriteInMs;
            logger.trace("SocketServer time spent on write per key {} = {}", transmission.getConnectionId(), writeTime);
        }
    }

    /**
     * Get the Transmission for the given connection
     */
    // TODO: 2018/3/27 by zmyer
    private Transmission getTransmission(SelectionKey key) {
        return (Transmission) key.attachment();
    }

    /**
     * Get the socket channel associated with this selection key
     */
    // TODO: 2018/3/27 by zmyer
    private SocketChannel channel(SelectionKey key) {
        return (SocketChannel) key.channel();
    }
}
