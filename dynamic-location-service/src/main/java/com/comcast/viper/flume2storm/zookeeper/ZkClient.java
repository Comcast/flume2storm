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
package com.comcast.viper.flume2storm.zookeeper;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around Zookeeper to facilitate usage, especially related to
 * connection management. The configuration can be changed at any time. The
 * change may not take effect necessarily right away, but on the next request.
 * The point of the ZkClient is to keep the connection to ZooKeeper alive and
 * well.
 */
public class ZkClient {
  protected static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);
  protected final AtomicReference<ZkClientConfiguration> config;
  protected final ReadWriteLock stateSync;
  protected ZkClientState state;
  // This state is related to the connection manager
  protected final AtomicBoolean started;
  protected final ZkClientListener zkClientListener;
  protected final Queue<ZkClientStateListener> stateListeners;
  protected ConnectionManager connectionManager;
  // protected final Object zkSync;
  protected final AtomicReference<ZooKeeper> zookeeper;

  /**
   * Construct a new ZkClient without listener
   */
  public ZkClient() {
    this(null);
  }

  /**
   * Construct a new ZkClient
   * 
   * @param listener
   *          The {@link ZkClientListener}. Can have only one of these, but it
   *          may be null.
   */
  public ZkClient(final ZkClientListener listener) {
    stateSync = new ReentrantReadWriteLock();
    state = ZkClientState.STOPPED;
    started = new AtomicBoolean(false);
    config = new AtomicReference<ZkClientConfiguration>(new ZkClientConfiguration());
    zkClientListener = listener;
    stateListeners = new ConcurrentLinkedQueue<ZkClientStateListener>();
    zookeeper = new AtomicReference<ZooKeeper>();
    // zkSync = new Object();
  }

  /**
   * @param configuration
   *          The new configuration to set
   */
  public void configure(final ZkClientConfiguration configuration) {
    config.set(configuration);
  }

  /**
   * Starts ZkClient
   * 
   * @return True if the ZkClient has been started (actually, merely requested
   *         to start). False if it was actually already started
   */
  public synchronized boolean start() {
    if (started.get()) {
      LOG.warn("Already started");
      return false;
    }
    LOG.debug("Starting...");
    started.set(true);
    connectionManager = new ConnectionManager();
    addStateListener(connectionManager);
    connectionManager.start();
    LOG.info("Started");
    return true;
  }

  /**
   * Stops ZkClient
   * 
   * @return True if the ZkClient has been stopped. False if it was actually
   *         already stopped
   */
  public synchronized boolean stop() {
    if (!started.get()) {
      LOG.warn("Already stopped");
      return false;
    }
    LOG.debug("Stopping...");
    started.set(false);
    try {
      if (connectionManager != null) {
        connectionManager.interrupt();
        connectionManager.join(config.get().getTerminationTimeout());
      }
    } catch (final InterruptedException e) {
      LOG.error("Failed to stop the connection manager thread");
    }
    removeStateListener(connectionManager);
    LOG.info("Stopped");
    return true;
  }

  /**
   * @return The current configuration
   */
  public ZkClientConfiguration getConfiguration() {
    return config.get();
  }

  //
  // State management
  //

  protected void setState(final ZkClientState newState) {
    final ZkClientState oldState;
    try {
      stateSync.writeLock().lock();
      assert state != null : "The state is never null";
      // No change?
      if (state.equals(newState)) {
        LOG.debug("State unchanged to: {}", newState);
        return;
      }
      // Recording the state change
      oldState = state;
      state = newState;
    } finally {
      stateSync.writeLock().unlock();
    }
    LOG.debug("State changed: {} -> {}", oldState, newState);
    // Calling the listeners
    assert oldState != null : "The old state is never null either";
    for (final ZkClientStateListener listener : stateListeners) {
      listener.onStateChange(oldState, newState);
    }
  }

  /**
   * @return The current state of the {@link ZkClient}
   */
  public ZkClientState getState() {
    try {
      stateSync.readLock().lock();
      return state;
    } finally {
      stateSync.readLock().unlock();
    }
  }

  /**
   * @return The session timeout once negotiated with the ZK server. If not
   *         connected to the server, it returns null
   */
  public Integer getNegotiatedSessionTimeout() {
    if (!getState().isStarted()) {
      return null;
    }
    final ZkSession session = connectionManager.currentSession.get();
    return session != null ? session.getTimeout() : null;
  }

  /**
   * Be careful with this please!
   * 
   * @return The {@link ZooKeeper}, or null if not active
   */
  protected ZooKeeper getZooKeeper() {
    return zookeeper.get();
  }

  /**
   * @param listener
   *          The state listener to add
   */
  public void addStateListener(final ZkClientStateListener listener) {
    stateListeners.add(listener);
  }

  /**
   * @param listener
   *          The state listener to remove
   */
  public void removeStateListener(final ZkClientStateListener listener) {
    stateListeners.remove(listener);
  }

  //
  // Connection management (where the fun begins...)
  //

  /**
   * The connection manager thread. <br />
   * The way ZK connection seems to work is that there are 2 sockets being
   * established, but only on the second one does the session gets actually
   * fully created. This means that sometimes, the getSessionId call returns 0
   * for up to a couple of seconds (the time for the second socket to be
   * established). So in the main loop thread (i.e. ConnectionManager), we wait
   * for the session to be fully established before continuing.
   */
  protected class ConnectionManager extends Thread implements ZkClientStateListener {
    // Programming note: current session NEVER points to null and ALWAYS to
    // an immutable (as in "thread-safe") object
    protected final AtomicReference<ZkSession> currentSession;

    public ConnectionManager() {
      setName("ConnectionManager");
      setDaemon(true);
      currentSession = new AtomicReference<ZkSession>(ZkSession.NO_SESSION);
    }

    private final void setZkSession() {
      final ZooKeeper zk = zookeeper.get();
      if (zk != null) {
        currentSession.set(ZkSession.build(zk.getSessionId(), zk.getSessionPasswd(), zk.getSessionTimeout()));
        LOG.debug("Stored Zk Session: {}", currentSession);
      }
    }

    protected final void clearZkSession() {
      if (currentSession.get().isSet()) {
        LOG.debug("Clearing Zk Session: {}", currentSession);
        currentSession.set(ZkSession.NO_SESSION);
      }
    }

    /**
     * This is the connection manager thread, where we mostly wait (so that we
     * don't do it in the event thread - i.e. in the connection watcher process
     * method)
     * 
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      LOG.info("Thread started");
      boolean exitMainLoop = false;
      while (!exitMainLoop) {
        try {
          switch (ZkClient.this.getState()) {
            case STOPPED:
              setState(ZkClientState.CONNECTING);
              break;

            //
            case CONNECTING:
              if (started.get()) {
                try {
                  Thread.sleep(100);
                } catch (final InterruptedException e) {
                  // Nothing to do
                }
              } else {
                /*
                 * Programming note: we didn't initialize, so we shouldn't call
                 * the clean up
                 */
                exitMainLoop = true;
              }
              break;

            //
            case CONNECTED:
              if (started.get()) {
                final long t0 = System.currentTimeMillis();
                while (zookeeper.get().getSessionId() == 0
                    && System.currentTimeMillis() - t0 < config.get().getConnectionTimeout() && started.get()) {
                  Thread.sleep(100);
                }
                if (zookeeper.get().getSessionId() == 0) {
                  LOG.error("Failed to retrieve Zookeeper session id");
                  setState(ZkClientState.DISCONNECTING);
                } else {
                  final boolean isRecovery = currentSession.get().sameAs(zookeeper.get().getSessionId());
                  setZkSession();
                  if (isRecovery) {
                    setState(ZkClientState.RECOVERING);
                  } else {
                    setState(ZkClientState.INITIALIZING);
                  }
                }
              } else {
                /*
                 * Programming note: we didn't initialize, so we shouldn't call
                 * the clean up
                 */
                exitMainLoop = true;
              }
              break;

            //
            case INITIALIZING:
              try {
                if (zkClientListener != null) {
                  zkClientListener.initialize();
                }
                setState(ZkClientState.SETUP);
              } catch (final Exception e) {
                LOG.error("Failed to initialize ZkClient", e);
                setState(ZkClientState.DISCONNECTING);
              }
              break;

            //
            case RECOVERING:
              LOG.debug("Using Zk session: {}", currentSession);
              setState(ZkClientState.SETUP);
              break;

            //
            case SETUP:
              if (started.get()) {
                try {
                  Thread.sleep(100);
                } catch (final InterruptedException e) {
                  // Nothing to do
                }
              } else {
                setState(ZkClientState.CLEANING_UP);
              }
              break;

            //
            case CLEANING_UP:
              try {
                if (zkClientListener != null) {
                  zkClientListener.terminate();
                }
              } catch (final Exception e) {
                LOG.error("Failed to clean up ZkClient", e);
              }
              setState(ZkClientState.DISCONNECTING);
              break;

            //
            case DISCONNECTING:
              try {
                disconnect();
                clearZkSession();
              } catch (final Exception e) {
                LOG.error("Failed to disconnect properly ZkClient from ZK quorum", e);
              }
              setState(ZkClientState.DISCONNECTED);
              break;

            //
            case DISCONNECTED:
              if (started.get()) {
                try {
                  Thread.sleep(config.get().getReconnectionDelay());
                } catch (final InterruptedException e) {
                  // Nothing to do
                }
              } else {
                exitMainLoop = true;
              }
              break;

            //
            default:
              throw new AssertionError("Forgetting states?");
          }
        } catch (final Exception e) {
          LOG.error("Unexpected failure: " + e.getLocalizedMessage(), e);
        }
      }
      setState(ZkClientState.STOPPED);
      LOG.info("Thread terminated");
    }

    /**
     * @see com.comcast.viper.flume2storm.zookeeper.ZkClientStateListener#onStateChange(com.comcast.viper.flume2storm.zookeeper.ZkClientState,
     *      com.comcast.viper.flume2storm.zookeeper.ZkClientState)
     */
    @Override
    public void onStateChange(final ZkClientState previousState, final ZkClientState newState) {
      switch (newState) {
        case CONNECTING:
          // Programming note: the connected state is reached using ZK
          // event
          if (!connect()) {
            setState(ZkClientState.DISCONNECTED);
          }
          break;

        case CONNECTED:
          if (zkClientListener != null) {
            zkClientListener.onConnection();
          }
          break;

        case RECOVERING:
          if (zkClientListener != null) {
            zkClientListener.onConnection();
          }
          break;

        case DISCONNECTED:
          if (zkClientListener != null) {
            zkClientListener.onDisconnection();
          }
          break;

        default:
          break;
      }
    }

    protected boolean connect() {
      final ZkClientConfiguration configuration = config.get();
      final ZkSession session = ZkSession.copyOf(currentSession.get());
      try {
        if (session.isSet()) {
          LOG.debug("Reconnecting to ZK server using old session: {}...", session);
          zookeeper.set(new ZooKeeper(configuration.getConnectionStr(), configuration.getSessionTimeout(),
              connectionWatcher, session.getSessionId(), session.getSessionPwd()));
        } else {
          LOG.debug("Connecting to ZK server using new session...");
          zookeeper.set(new ZooKeeper(configuration.getConnectionStr(), configuration.getSessionTimeout(),
              connectionWatcher));
        }
        return true;
      } catch (final IOException e) {
        LOG.error("Failed to connection ZK quorum (to: " + configuration.getConnectionStr() + ")", e);
        return false;
      }
    }

    // TODO verify that the connection watcher keeps triggering after
    // connection

    protected final Watcher connectionWatcher = new Watcher() {
      /**
       * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
       */
      @Override
      public void process(final WatchedEvent event) {
        LOG.debug("Got ZK event: {} from thread id: {}", event, Thread.currentThread().getId());
        switch (event.getState()) {
          case SyncConnected:
            switch (ZkClient.this.getState()) {
              case CONNECTING:
                /*
                 * Regular connection: our (ZkClient) connection attempt has
                 * been successful
                 */
                setState(ZkClientState.CONNECTED);
                break;

              case DISCONNECTED:
                /*
                 * Regular zookeeper reconnection: The zookeeper client
                 * re-established the connection by itself
                 */
                setState(ZkClientState.RECOVERING);
                break;

              default:
                // Ignoring all other cases
                break;
            }
            break;

          case Disconnected:
            setState(ZkClientState.DISCONNECTED);
            break;

          case Expired:
            /*
             * The session expired - this may happen when something went wrong
             * in the comm between the client and the server (but both were
             * still running). Zookeeper will attempt to reconnect
             */
            LOG.warn("*** Detected session expiration, reconnecting...");
            clearZkSession();
            if (ZkClient.this.getState() == ZkClientState.DISCONNECTED) {
              setState(ZkClientState.CONNECTING);
            }
            break;

          case AuthFailed:
            LOG.error("Authorization failure: {}", event);
            break;

          default:
            throw new AssertionError("whoops!");
        }
      }
    };

    protected void disconnect() {
      try {
        final ZooKeeper zk = zookeeper.get();
        if (zk != null) {
          zk.close();
        }
      } catch (final InterruptedException e) {
        LOG.error("Failed to disconnection ZK quorum", e);
      }
    }
  }
}
