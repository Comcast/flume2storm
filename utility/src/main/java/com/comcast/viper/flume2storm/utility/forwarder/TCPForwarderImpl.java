/**
 * Copyright 2014 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.viper.flume2storm.utility.forwarder;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of the {@link TCPForwarder} based on Java NIO library
 * that link 1 input socket to 1 output socket. We synchronize outside
 * operations (start/stop/freeze/resume) in order to avoid potential armful
 * state changes.
 */
class TCPForwarderImpl implements TCPForwarder {
  protected static final Logger LOG = LoggerFactory.getLogger(TCPForwarder.class);
  private static final String MAIN_THREAD_NAME = "TCPForwarderAcceptThread";
  private static final long TERMINATION_TIMEOUT = 10000;
  private static final int MAX_TCP_PACKET_SIZE = 64 * 1024;

  private final TCPForwarderConfig configuration;
  private final AtomicReference<MyState> state;
  private MainThread mainThread;
  private final AtomicInteger connectionDelay;
  private final AtomicInteger clientSendDelay;
  private final AtomicInteger serverSendDelay;

  enum MyState {
    STOPPED,
    STARTING,
    STARTED,
    FROZEN,
    STOPPING;
  }

  class MainThread extends Thread {
    private final ExecutorService executor;
    private final ConcurrentLinkedQueue<SocketForwarder> socketForwarders;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;

    public MainThread() {
      setName(MAIN_THREAD_NAME);
      setDaemon(true);
      executor = Executors.newFixedThreadPool(configuration.getMaxWorkerThread());
      socketForwarders = new ConcurrentLinkedQueue<SocketForwarder>();
    }

    private void wakeup() {
      if (selector != null)
        selector.wakeup();
    }

    protected void onSocketForwarderTerminated(final SocketForwarder sf) {
      socketForwarders.remove(sf);
    }

    @Override
    public void run() {
      try {
        LOG.debug("Thread started");
        open(configuration.getListenAddress(), configuration.getInputPort());
        setState(MyState.STARTED);

        boolean terminate = false;
        while (!terminate) {
          switch (state.get()) {
            case FROZEN:
              try {
                Thread.sleep(50);
              } catch (final InterruptedException e) {
                LOG.debug("Interrupted");
              }
              break;

            case STOPPING:
            case STOPPED:
              terminate = true;
              break;

            case STARTED:
              LOG.trace("Waiting for new connection....");
              if (selector.select(100) > 0) {
                LOG.trace("Processing potential connection event...");
                final Iterator<SelectionKey> i = selector.selectedKeys().iterator();
                while (i.hasNext()) {
                  final SelectionKey sk = i.next();
                  i.remove();
                  if (!sk.isValid()) {
                    LOG.warn("Skipping invalid event");
                  } else if (sk.isAcceptable()) {
                    handleConnection(sk);
                  }
                }
              }
              break;

            case STARTING:
              assert false : "This is unlikely...";
              break;

            default:
              throw new AssertionError("Forgetting state?");
          }
        }
        LOG.debug("Exiting main thead loop...");
      } catch (final BindException e) {
        LOG.error("Failed to bind socket: " + e.getLocalizedMessage(), e);
      } catch (final Exception e) {
        LOG.error("Processing exception: " + e.getMessage(), e);
      } finally {
        close();
        setState(MyState.STOPPED);
        LOG.info("Thread terminated");
      }
    }

    private void open(final String serverAddress, final int serverPort) throws IOException {
      final String logServerAddress = serverAddress == null ? "0.0.0.0" : serverAddress;
      LOG.debug("Opening server socket on {} TCP port {}...", logServerAddress, serverPort);
      selector = SelectorProvider.provider().openSelector();
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);

      InetSocketAddress isa = null;
      if (serverAddress == null) {
        isa = new InetSocketAddress(serverPort);
      } else {
        isa = new InetSocketAddress(serverAddress, serverPort);
      }
      serverSocketChannel.socket().bind(isa);
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
      LOG.info("Listening on {} TCP port {}", logServerAddress, serverPort);
    }

    private void close() {
      // Closing server socket channel
      if (serverSocketChannel != null) {
        try {
          LOG.debug("Closing server socket channel...");
          serverSocketChannel.close();
        } catch (final IOException e) {
          LOG.error("Failed to close server socket channel", e);
        }
      }
      if (selector != null) {
        try {
          LOG.debug("Closing server socket selector...");
          selector.close();
        } catch (final IOException e) {
          LOG.error("Failed to close server socket selector", e);
        }
      }

      // Waking up all the SocketForwarder. Each of them should terminate
      // and remove themselves from the list
      executor.shutdown();
      if (!socketForwarders.isEmpty()) {
        LOG.debug("Shutting down SocketForwarders...");
        for (final SocketForwarder sf : socketForwarders) {
          sf.wakeup();
        }
      }
      LOG.debug("Waiting for executor termination...");
      try {
        executor.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        // Nothing to see here
      }
      socketForwarders.clear();
      LOG.debug("Executor terminated");
    }

    private void handleConnection(final SelectionKey sk) {
      // Delay while connecting
      final int d = connectionDelay.get();
      if (d > 0) {
        LOG.trace("Waiting {} ms before accepting connection...", d);
        try {
          Thread.sleep(d);
        } catch (final InterruptedException e) {
          LOG.info("Connection wait interrupted");
        }
      }

      // Received connection attempt. Trying to connect output server
      SocketChannel outputSocketChannel = null;
      try {
        LOG.debug("Received connection event, opening socket to output server {}:{}...",
            configuration.getOutputServer(), configuration.getOutputPort());
        outputSocketChannel = SocketChannel.open();
        outputSocketChannel.configureBlocking(true);
        final InetSocketAddress outputAddress = new InetSocketAddress(configuration.getOutputServer(),
            configuration.getOutputPort());
        outputSocketChannel.connect(outputAddress);
        outputSocketChannel.configureBlocking(false);
        LOG.info("Connected output server: {}", getFullAddress(outputSocketChannel, Direction.OUTGOING));
      } catch (final IOException e) {
        LOG.error("Failed to connect output server: " + e.getMessage(), e);
        if (outputSocketChannel != null) {
          LOG.debug("Closing output connection to server...");
          try {
            outputSocketChannel.close();
          } catch (final IOException e2) {
            LOG.warn("Failed to close output connection to server: " + e.getMessage(), e);
          } finally {
            outputSocketChannel = null;
          }
        }
      }

      // Accepting incoming connection
      SocketChannel inputSocketChannel = null;
      try {
        // Programming note: This needs to be done whether or not the
        // output connection has been established
        inputSocketChannel = serverSocketChannel.accept();
        inputSocketChannel.configureBlocking(false);
        LOG.info("Accepted connection from {}", inputSocketChannel.socket().getRemoteSocketAddress());
      } catch (final Exception e) {
        LOG.error("Failed to accept incoming connection: " + e.getMessage(), e);
      }

      // server socket failed - closing input connection
      if (outputSocketChannel == null && inputSocketChannel != null) {
        try {
          inputSocketChannel.close();
        } catch (final IOException e) {
          LOG.error(
              "Failed to close input connection following output connection establishment failure:" + e.getMessage(), e);
        }
      }

      // Linking the 2 sockets
      if (inputSocketChannel != null && outputSocketChannel != null) {
        try {
          LOG.debug("Linking input {} and output {} channels...",
              getFullAddress(inputSocketChannel, Direction.INCOMING),
              getFullAddress(outputSocketChannel, Direction.OUTGOING));
          final SocketForwarder inputSocketForwarder = new SocketForwarder(inputSocketChannel, Direction.INCOMING);
          socketForwarders.add(inputSocketForwarder);
          final SocketForwarder outputSocketForwarder = new SocketForwarder(outputSocketChannel, Direction.OUTGOING);
          socketForwarders.add(outputSocketForwarder);
          inputSocketForwarder.bindTo(outputSocketForwarder);
          outputSocketForwarder.bindTo(inputSocketForwarder);
          executor.execute(inputSocketForwarder);
          executor.execute(outputSocketForwarder);
          LOG.info("Linked input {} and output {} channels", getFullAddress(inputSocketChannel, Direction.INCOMING),
              getFullAddress(outputSocketChannel, Direction.OUTGOING));
        } catch (final Exception e) {
          LOG.error("Failed to link input and output channels: " + e.getMessage(), e);
        }
      }
    }
  }

  private enum Direction {
    INCOMING,
    OUTGOING
  }

  private static String getFullAddress(final SocketChannel channel, final Direction direction) {
    try {
      String origin, destination;
      if (direction == Direction.INCOMING) {
        origin = channel.socket().getRemoteSocketAddress().toString();
        destination = channel.socket().getLocalSocketAddress().toString();
      } else {
        origin = channel.socket().getLocalSocketAddress().toString();
        destination = channel.socket().getRemoteSocketAddress().toString();
      }
      return new StringBuilder("'").append(origin).append("->").append(destination).append("'").toString();
    } catch (Exception e) {
      return "[Unknown]";
    }
  }

  /**
   * This reads from a channel and forward what is received to the channel bound
   * to it
   */
  class SocketForwarder implements Runnable {
    private final Logger log;
    private final Selector selector;
    private final SocketChannel socketChannel;
    private final AtomicReference<SocketForwarder> boundTo;
    private final AtomicInteger delay;
    private final AtomicBoolean terminate;
    private final Direction direction;

    public SocketForwarder(final SocketChannel socketChannel, final Direction type) throws IOException {
      assert socketChannel != null;
      log = LoggerFactory.getLogger(SocketForwarder.class + "." + getFullAddress(socketChannel, type));
      selector = SelectorProvider.provider().openSelector();
      this.socketChannel = socketChannel;
      this.delay = type == Direction.INCOMING ? clientSendDelay : serverSendDelay;
      boundTo = new AtomicReference<SocketForwarder>();
      terminate = new AtomicBoolean(false);
      this.direction = type;
    }

    protected void wakeup() {
      selector.wakeup();
    }

    protected void bindTo(final SocketForwarder other) {
      boundTo.set(other);
    }

    /**
     * @return The {@link SocketForwarder} is was bound to
     */
    protected SocketForwarder unbind() {
      return boundTo.getAndSet(null);
    }

    /**
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
      Thread.currentThread().setName(getFullAddress(socketChannel, direction));
      try {
        socketChannel.register(this.selector, SelectionKey.OP_READ);
        while (!terminate.get()) {
          switch (state.get()) {
            case FROZEN:
              try {
                Thread.sleep(100);
              } catch (final InterruptedException e) {
                log.debug("Interrupted");
              }
              break;

            case STOPPING:
            case STOPPED:
              terminate.set(true);
              break;

            case STARTED:
              log.trace("Waiting for socket event....");
              int nbKeys = selector.select(50);
              if (nbKeys == 0) {
                log.trace("Forwarder has no keys... continuing");
              } else {
                log.trace("Processing potential socket event...");
                final Iterator<SelectionKey> i = selector.selectedKeys().iterator();
                while (i.hasNext()) {
                  final SelectionKey sk = i.next();
                  i.remove();
                  if (!sk.isValid()) {
                    log.warn("Skipping invalid selection key");
                  } else if (sk.isReadable()) {
                    doRead(sk);
                  }
                }
              }
              break;

            case STARTING:
              assert false : "This is unlikely...";
              break;

            default:
              throw new AssertionError("Forgetting state?");
          }
        }
      } catch (final Exception e) {
        log.error("Processing exception: " + e.getMessage(), e);
      } finally {
        close();
        log.info("Task terminated");
      }
    }

    private void doRead(final SelectionKey sk) {
      try {
        final ByteBuffer readBuffer = ByteBuffer.allocate(MAX_TCP_PACKET_SIZE);
        if (socketChannel.read(readBuffer) < 0) {
          disconnect();
        } else {
          readBuffer.flip();
          if (boundTo != null) {
            boundTo.get().send(readBuffer, delay.get());
          }
        }
      } catch (final Exception e) {
        log.error("Failed to read socket: " + e.getMessage(), e);
      }
    }

    protected void send(final ByteBuffer toSend, final int sendDelay) {
      mainThread.executor.execute(new Runnable() {
        @Override
        public void run() {
          Thread.currentThread().setName(socketChannel.socket().getRemoteSocketAddress() + "-sender");
          try {
            // Delay while sending (i.e. forwarding data)
            if (sendDelay > 0) {
              Thread.sleep(sendDelay);
            }
            // Programming note: Taking a shortcut assuming the
            // socket is always ready to write to
            if (socketChannel.isOpen() && socketChannel.isConnected()) {
              socketChannel.write(toSend);
              selector.wakeup();
            }
          } catch (final Exception e) {
            log.error("Failed to send data: " + e.getMessage(), e);
          }
        }
      });
    }

    protected void disconnect() {
      terminate.set(true);
      // Closing socket
      log.debug("Socket {} disconnected. Closing channel", getFullAddress(socketChannel, direction));
      try {
        socketChannel.close();
        log.info("Channel for socket {} closed", socketChannel.socket().getRemoteSocketAddress());
      } catch (final Exception e) {
        log.error("Failed to close channel for socket {}: {}", getFullAddress(socketChannel, direction), e.getMessage());
      }
    }

    private void close() {
      log.debug("Closing...");
      try {
        disconnect();
        final SocketForwarder other = unbind();
        if (other != null) {
          other.unbind();
          other.disconnect();
        }
      } catch (final Exception e) {
        log.error("Closing SocketForwarder failed: " + e.getMessage(), e);
      }
      try {
        selector.close();
      } catch (final IOException e) {
        log.error("Closing selector failed: " + e.getMessage(), e);
      }
      mainThread.onSocketForwarderTerminated(this);
    }
  }

  /**
   * @param config
   *          The {@link TCPForwarder} configuration
   */
  public TCPForwarderImpl(final TCPForwarderConfig config) {
    configuration = TCPForwarderConfig.copyOf(config);
    state = new AtomicReference<MyState>(MyState.STOPPED);
    connectionDelay = new AtomicInteger(0);
    clientSendDelay = new AtomicInteger(0);
    serverSendDelay = new AtomicInteger(0);
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#start()
   */
  @Override
  public synchronized void start() {
    if (state.get() != MyState.STOPPED) {
      LOG.warn("Invalid state while trying to start: {}", state.get());
      return;
    }
    LOG.debug("Starting...");
    setState(MyState.STARTING);
    mainThread = new MainThread();
    mainThread.start();
    LOG.info("Start requested");
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#stop()
   */
  @Override
  public synchronized void stop() {
    if (state.get() == MyState.STOPPED || state.get() == MyState.STOPPING) {
      LOG.warn("Invalid state while trying to stop: {}", state.get());
      return;
    }
    LOG.debug("Stopping...");
    setState(MyState.STOPPING);
    if (mainThread != null) {
      mainThread.wakeup();
      try {
        mainThread.join(TERMINATION_TIMEOUT);
      } catch (final InterruptedException e) {
        LOG.warn("Interrupted while waiting for {} termination", MAIN_THREAD_NAME);
      }
    }
    setState(MyState.STOPPED);
    LOG.info("Stopped");
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#freeze()
   */
  @Override
  public synchronized void freeze() {
    switch (state.get()) {
      case FROZEN:
        LOG.warn("Already frozen");
        return;

      case STOPPED:
      case STOPPING:
        LOG.warn("Invalid state while trying to freeze: {}", state.get());
        return;

      default:
        LOG.info("Freezing...");
        setState(MyState.FROZEN);
        if (mainThread != null) {
          mainThread.wakeup();
        }
        break;
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#resume()
   */
  @Override
  public synchronized void resume() {
    switch (state.get()) {
      case FROZEN:
        LOG.info("Resuming...");
        setState(MyState.STARTED);
        return;

      default:
        LOG.warn("Invalid state while trying to resume: {}", state.get());
        break;
    }
  }

  private void setState(final MyState newState) {
    final MyState oldState = state.getAndSet(newState);
    LOG.debug("State change: {} -> {}", oldState, newState);
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#isActive()
   */
  @Override
  public boolean isActive() {
    return state.get() == MyState.STARTED;
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#isFrozen()
   */
  @Override
  public boolean isFrozen() {
    return state.get() == MyState.FROZEN;
  }

  //
  // Delay management
  //

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#resetDelay()
   */
  @Override
  public void resetDelay() {
    connectionDelay.set(0);
    clientSendDelay.set(0);
    serverSendDelay.set(0);
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#getConnectionDelay()
   */
  @Override
  public int getConnectionDelay() {
    return connectionDelay.get();
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#setConnectionDelay(int)
   */
  @Override
  public TCPForwarder setConnectionDelay(final int delay) {
    assert delay >= 0 : "Invalid value while setting connection delay (must be positive)";
    connectionDelay.set(delay);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#getClientSendDelay()
   */
  @Override
  public int getClientSendDelay() {
    return clientSendDelay.get();
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#setClientSendDelay(int)
   */
  @Override
  public TCPForwarder setClientSendDelay(final int delay) {
    assert delay >= 0 : "Invalid value while setting client send delay (must be positive)";
    clientSendDelay.set(delay);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#getServerSendDelay()
   */
  @Override
  public int getServerSendDelay() {
    return serverSendDelay.get();
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.forwarder.test.forwarder.TCPForwarder#setServerSendDelay(int)
   */
  @Override
  public TCPForwarder setServerSendDelay(final int delay) {
    assert delay >= 0 : "Invalid value while setting server send delay (must be positive)";
    serverSendDelay.set(delay);
    return this;
  }
}
