package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A selector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link BoundedByteBufferSend} and {@link BoundedByteBufferReceive} to
 * transmit network requests and responses.
 * <p>
 * A connection can be added to the selector by doing
 *
 * <pre>
 * long id = selector.connect(new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
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
class Selector implements Selectable {

  private static final Logger logger = LoggerFactory.getLogger(Selector.class);

  private final java.nio.channels.Selector selector;
  private final Map<Long, SelectionKey> keys;
  private final List<NetworkSend> completedSends;
  private final List<NetworkReceive> completedReceives;
  private final List<Long> disconnected;
  private final List<Long> connected;
  private final Time time;
  private final NetworkMetrics metrics;
  private final AtomicLong IdGenerator;
  private AtomicLong activeConnections;

  /**
   * Create a new selector
   */
  public Selector(NetworkMetrics metrics, Time time)
      throws IOException {
    this.selector = java.nio.channels.Selector.open();
    this.time = time;
    this.keys = new HashMap<Long, SelectionKey>();
    this.completedSends = new ArrayList<NetworkSend>();
    this.completedReceives = new ArrayList<NetworkReceive>();
    this.connected = new ArrayList<Long>();
    this.disconnected = new ArrayList<Long>();
    this.IdGenerator = new AtomicLong(0);
    this.metrics = metrics;
    this.activeConnections = new AtomicLong(0);
    this.metrics.initializeSelectorMetricsIfRequired(activeConnections);
  }

  /**
   * Begin connecting to the given address and add the connection to this selector and returns an id that identifies
   * the connection
   * <p>
   * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long, List)}
   * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
   * @param address The address to connect to
   * @param sendBufferSize The send buffer for the new connection
   * @param receiveBufferSize The receive buffer for the new connection
   * @return The id for the connection that was created
   * @throws IllegalStateException if there is already a connection for that id
   * @throws IOException if DNS resolution fails on the hostname or if the server is down
   */
  @Override
  public long connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize)
      throws IOException {
    SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(false);
    Socket socket = channel.socket();
    socket.setKeepAlive(true);
    socket.setSendBufferSize(sendBufferSize);
    socket.setReceiveBufferSize(receiveBufferSize);
    socket.setTcpNoDelay(true);
    try {
      channel.connect(address);
    } catch (UnresolvedAddressException e) {
      channel.close();
      throw new IOException("Can't resolve address: " + address, e);
    } catch (IOException e) {
      channel.close();
      throw e;
    }
    SelectionKey key = channel.register(this.selector, SelectionKey.OP_CONNECT);
    long connectionId = IdGenerator.getAndIncrement();
    key.attach(new Transmissions(connectionId, address.getHostName(), address.getPort()));
    this.keys.put(connectionId, key);
    activeConnections.set(this.keys.size());
    return connectionId;
  }

  /**
   * Disconnect any connections for the given id (if there are any). The disconnection is asynchronous and will not be
   * processed until the next {@link #poll(long, List) poll()} call.
   */
  @Override
  public void disconnect(long id) {
    SelectionKey key = this.keys.get(id);
    if (key != null) {
      key.cancel();
    }
  }

  /**
   * Interrupt the selector if it is blocked waiting to do I/O.
   */
  @Override
  public void wakeup() {
    this.selector.wakeup();
  }

  /**
   * Close this selector and all associated connections
   */
  @Override
  public void close() {
    for (SelectionKey key : this.selector.keys()) {
      close(key);
    }
    try {
      this.selector.close();
    } catch (IOException e) {
      logger.error("Exception closing selector:", e);
    }
  }

  /**
   * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
   * disconnections, initiating new sends, or making progress on in-progress sends or receives.
   * <p>
   * The provided sends will be started.
   *
   * When this call is completed the user can check for completed sends, receives, connections or disconnects using
   * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
   * lists will be cleared at the beginning of each {@link #poll(long, List)} call and repopulated by the call if any
   * completed I/O.
   *
   * @param timeout The amount of time to wait, in milliseconds. If negative, wait indefinitely.
   * @param sends The list of new sends to begin
   *
   * @throws IOException If a send is given for which we have no existing connection or for which there is
   *         already an in-progress send
   */
  @Override
  public void poll(long timeout, List<NetworkSend> sends)
      throws IOException {
    clear();

    // register for write interest on any new sends
    for (NetworkSend send : sends) {
      SelectionKey key = keyForId(send.getConnectionId());
      Transmissions lastTransmission = transmissions(key);
      if (lastTransmission.hasSend()) {
        throw new IllegalStateException(
            "Attempt to begin a send operation with prior send operation still in progress.");
      }
      lastTransmission.send = send;
      try {
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
      } catch (CancelledKeyException e) {
        close(key);
      }
    }

    // check ready keys
    long startSelect = time.nanoseconds();
    int readyKeys = select(timeout);
    long endSelect = time.nanoseconds();
    this.metrics.selectorSelectTime.update(endSelect - startSelect);
    this.metrics.selectorSelectRate.inc();

    if (readyKeys > 0) {
      Set<SelectionKey> keys = this.selector.selectedKeys();
      Iterator<SelectionKey> iter = keys.iterator();
      while (iter.hasNext()) {
        SelectionKey key = iter.next();
        iter.remove();

        Transmissions transmissions = transmissions(key);
        SocketChannel channel = channel(key);

        // register all per-node metrics at once
        metrics.initializeSelectorNodeMetricIfRequired(transmissions.remoteHostName, transmissions.remotePort);

        try {
          // complete any connections that have finished their handshake
          if (key.isConnectable()) {
            channel.finishConnect();
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
            this.connected.add(transmissions.getConnectionId());
            this.metrics.selectorConnectionCreated.inc();
            this.metrics.initializeSelectorNodeMetricIfRequired(transmissions.remoteHostName, transmissions.remotePort);
          }

          // read from any connections that have readable data
          if (key.isReadable()) {
            if (!transmissions.hasReceive()) {
              transmissions.receive =
                  new NetworkReceive(transmissions.getConnectionId(), new BoundedByteBufferReceive(),
                      SystemTime.getInstance());
            }
            long bytesRead = transmissions.receive.getReceivedBytes().readFrom(channel);
            if (bytesRead > 0) {
              metrics.selectorBytesReceived.update(bytesRead);
              metrics.selectorBytesReceivedCount.inc(bytesRead);
            }
            if (transmissions.receive.getReceivedBytes().isReadComplete()) {
              this.completedReceives.add(transmissions.receive);
              metrics.updateNodeResponseMetric(transmissions.remoteHostName, transmissions.remotePort,
                  transmissions.receive.getReceivedBytes().getPayload().limit(),
                  time.nanoseconds() - transmissions.receive.getReceiveStartTimeInNanos());
              transmissions.clearReceive();
            }
          }

          // write to any sockets that have space in their buffer and for which we have data
          if (key.isWritable()) {
            transmissions.send.getBytesToSend().writeTo(channel);
            if (transmissions.send.getBytesToSend().isSendComplete()) {
              this.completedSends.add(transmissions.send);
              metrics.updateNodeRequestMetric(transmissions.remoteHostName, transmissions.remotePort,
                  transmissions.send.getBytesToSend().sizeInBytes(),
                  SystemTime.getInstance().nanoseconds() - transmissions.send.getSendStartTimeInNanos());
              transmissions.clearSend();
              key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
          }

          // cancel any defunct sockets
          if (!key.isValid()) {
            close(key);
          }
        } catch (IOException e) {
          InetAddress remoteAddress = null;
          Socket socket = channel.socket();
          if (socket != null) {
            remoteAddress = socket.getInetAddress();
          }
          logger.warn("Error in I/O with " + remoteAddress, e);
          close(key);
        }
      }
      this.metrics.selectorIORate.inc();
    }
    long endIo = time.nanoseconds();
    this.metrics.selectorIOTime.update(endIo);
  }

  @Override
  public List<NetworkSend> completedSends() {
    return this.completedSends;
  }

  @Override
  public List<NetworkReceive> completedReceives() {
    return this.completedReceives;
  }

  @Override
  public List<Long> disconnected() {
    return this.disconnected;
  }

  @Override
  public List<Long> connected() {
    return this.connected;
  }

  public long getActiveConnections() {
    return activeConnections.get();
  }

  /**
   * Clear the results from the prior poll
   */
  private void clear() {
    this.completedSends.clear();
    this.completedReceives.clear();
    this.connected.clear();
    this.disconnected.clear();
  }

  /**
   * Check for data, waiting up to the given timeout.
   *
   * @param ms Length of time to wait, in milliseconds. If negative, wait indefinitely.
   * @return The number of keys ready
   * @throws IOException
   */
  private int select(long ms)
      throws IOException {
    if (ms == 0L) {
      return this.selector.selectNow();
    } else if (ms < 0L) {
      return this.selector.select();
    } else {
      return this.selector.select(ms);
    }
  }

  /**
   * Begin closing this connection
   */
  private void close(SelectionKey key) {
    SocketChannel channel = channel(key);
    Transmissions trans = transmissions(key);
    if (trans != null) {
      this.disconnected.add(trans.connectionId);
      this.keys.remove(trans.connectionId);
      activeConnections.set(this.keys.size());
      trans.clearReceive();
      trans.clearSend();
    }
    key.attach(null);
    key.cancel();
    try {
      channel.socket().close();
      channel.close();
    } catch (IOException e) {
      logger.error("Exception closing connection to node {}:", trans.connectionId, e);
    }
    this.metrics.selectorConnectionClosed.inc();
  }

  /**
   * Get the selection key associated with this numeric id
   */
  private SelectionKey keyForId(long id) {
    SelectionKey key = this.keys.get(id);
    if (key == null) {
      throw new IllegalStateException("Attempt to write to socket for which there is no open connection.");
    }
    return key;
  }

  /**
   * Get the transmissions for the given connection
   */
  private Transmissions transmissions(SelectionKey key) {
    return (Transmissions) key.attachment();
  }

  /**
   * Get the socket channel associated with this selection key
   */
  private SocketChannel channel(SelectionKey key) {
    return (SocketChannel) key.channel();
  }

  /**
   * The id, hostname, port and in-progress send and receive associated with a connection
   */
  private static class Transmissions {
    public long connectionId;
    public String remoteHostName;
    public int remotePort;
    public NetworkSend send;
    public NetworkReceive receive;

    public Transmissions(long connectionId, String remoteHostName, int remotePort) {
      this.connectionId = connectionId;
      this.remoteHostName = remoteHostName;
      this.remotePort = remotePort;
    }

    public long getConnectionId() {
      return connectionId;
    }

    public boolean hasSend() {
      return this.send != null;
    }

    public void clearSend() {
      this.send = null;
    }

    public boolean hasReceive() {
      return this.receive != null;
    }

    public void clearReceive() {
      this.receive = null;
    }
  }
}

