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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;


/**
 * A blocking channel that is used to communicate with a server
 */
// TODO: 2018/3/23 by zmyer
public class BlockingChannel implements ConnectedChannel {
    //远端主机名
    protected final String host;
    //远端端口
    protected final int port;
    //读缓冲区大小
    protected final int readBufferSize;
    //写缓冲区大小
    protected final int writeBufferSize;
    //读超时时间
    protected final int readTimeoutMs;
    //连接超时时间
    protected final int connectTimeoutMs;
    //是否连接
    protected boolean connected = false;
    //读通道对象
    protected InputStream readChannel = null;
    //写通道对象
    protected WritableByteChannel writeChannel = null;
    //锁对象
    protected Object lock = new Object();
    //日志对象
    protected Logger logger = LoggerFactory.getLogger(getClass());
    //socket通道
    private SocketChannel channel = null;

    // TODO: 2018/3/27 by zmyer
    public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs,
            int connectTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
        this.connectTimeoutMs = connectTimeoutMs;
    }

    // TODO: 2018/3/27 by zmyer
    public void connect() throws IOException {
        synchronized (lock) {
            if (!connected) {
                //建立socket通道对象
                channel = SocketChannel.open();
                if (readBufferSize > 0) {
                    //设置读缓冲区大小
                    channel.socket().setReceiveBufferSize(readBufferSize);
                }
                if (writeBufferSize > 0) {
                    //设置写缓冲区大小
                    channel.socket().setSendBufferSize(writeBufferSize);
                }
                //设置阻塞
                channel.configureBlocking(true);
                //设置读超时时间
                channel.socket().setSoTimeout(readTimeoutMs);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                //开始连接
                channel.socket().connect(new InetSocketAddress(host, port), connectTimeoutMs);
                //设置写通道对象
                writeChannel = channel;
                //设置读通道对象
                readChannel = channel.socket().getInputStream();
                //连接建立成功
                connected = true;
                logger.debug("Created socket with SO_TIMEOUT = {} (requested {}), "
                                + "SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})",
                        channel.socket().getSoTimeout(),
                        readTimeoutMs, channel.socket().getReceiveBufferSize(), readBufferSize,
                        channel.socket().getSendBufferSize(), writeBufferSize);
            }
        }
    }

    // TODO: 2018/3/27 by zmyer
    public void disconnect() {
        synchronized (lock) {
            try {
                if (connected || channel != null) {
                    // closing the main socket channel *should* close the read channel
                    // but let's do it to be sure.
                    //关闭通道对象
                    channel.close();
                    //关闭socket
                    channel.socket().close();
                    if (readChannel != null) {
                        //关闭读通道对象
                        readChannel.close();
                        readChannel = null;
                    }
                    if (writeChannel != null) {
                        //关闭写通道对象
                        writeChannel.close();
                        writeChannel = null;
                    }
                    channel = null;
                    connected = false;
                }
            } catch (Exception e) {
                logger.error("error while disconnecting {}", e);
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    // TODO: 2018/3/27 by zmyer
    @Override
    public void send(Send request) throws IOException {
        if (!connected) {
            throw new ClosedChannelException();
        }
        while (!request.isSendComplete()) {
            //将消息写入通道对象中
            request.writeTo(writeChannel);
        }
    }

    // TODO: 2018/3/27 by zmyer
    @Override
    public ChannelOutput receive() throws IOException {
        if (!connected) {
            throw new ClosedChannelException();
        }

        // consume the size header and return the remaining response.
        //分配接受缓冲区
        ByteBuffer streamSizeBuffer = ByteBuffer.allocate(8);
        while (streamSizeBuffer.position() < streamSizeBuffer.capacity()) {
            //读取长度信息
            int read = readChannel.read();
            if (read == -1) {
                throw new IOException("Could not read complete size from readChannel ");
            }
            //将长度信息写入缓冲区
            streamSizeBuffer.put((byte) read);
        }
        //重置接受缓冲区
        streamSizeBuffer.flip();
        return new ChannelOutput(readChannel, streamSizeBuffer.getLong() - 8);
    }

    // TODO: 2018/3/27 by zmyer
    @Override
    public String getRemoteHost() {
        return host;
    }

    // TODO: 2018/3/27 by zmyer
    @Override
    public int getRemotePort() {
        return port;
    }
}