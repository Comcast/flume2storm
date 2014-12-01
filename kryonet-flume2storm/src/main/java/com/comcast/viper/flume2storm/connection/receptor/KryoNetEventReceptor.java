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
package com.comcast.viper.flume2storm.connection.receptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.KryoUtil;
import com.comcast.viper.flume2storm.connection.KryoNetParameters;
import com.comcast.viper.flume2storm.connection.MyKryoSerialization;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.connection.sender.KryoNetEventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage.KeepAlive;
import com.esotericsoftware.kryonet.Listener;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A Flume2Storm event receptor implementation using KryoNet framework. Note
 * that the receptor, when started, will always try to reconnect to the KryoNet
 * server. On disconnection, it will retry immediately, and then wait for a
 * specified retry delay before retrying again.
 */
public class KryoNetEventReceptor implements EventReceptor<KryoNetConnectionParameters> {
  protected static final Logger LOG = LoggerFactory.getLogger(KryoNetEventReceptor.class);
  protected final KryoNetParameters kryoNetParameters;
  protected final KryoNetConnectionParameters connectionParams;
  protected final EventReceptorStats stats;
  protected final AtomicReference<ImmutableList<EventReceptorListener>> listeners;
  protected final Queue<F2SEvent> events;
  protected final AtomicBoolean keepRunning;
  protected final AtomicReference<Instant> nextConnectionTime;
  protected MaintainConnection maintainConnectionThread;
  protected Client client;

  // TODO fix sleep time before reconnection attempt

  /**
   * Thread to maintain the connection to the KryoNet server open
   */
  protected class MaintainConnection extends Thread {
    /**
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      LOG.debug("Maintain connection thread started");
      try {
        while (keepRunning.get()) {
          if (!isConnected() && Instant.now().isAfter(nextConnectionTime.get())) {
            if (!connect()) {
              setNextReconnectionTime();
              LOG.trace("Connection failed. Set next connection attempt to {}", nextConnectionTime.get());
            }
          } else {
            try {
              Thread.sleep(100);
            } catch (final InterruptedException e) {
              // Nothing to do
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Exception in maintain thread: " + e.getMessage(), e);
      }
      cleanup();
      LOG.debug("Maintain connection thread terminated");
    }
  }

  /**
   * @param connectionParams
   *          Connection parameters to the {@link KryoNetEventSender}
   * @param kryoNetParams
   *          Generic parameters of the {@link KryoNetEventReceptor}
   */
  public KryoNetEventReceptor(final KryoNetConnectionParameters connectionParams, KryoNetParameters kryoNetParams) {
    Preconditions.checkNotNull(connectionParams);
    this.kryoNetParameters = kryoNetParams;
    this.connectionParams = connectionParams;
    stats = new EventReceptorStats(connectionParams.getId());
    listeners = new AtomicReference<>(ImmutableList.copyOf(new ArrayList<EventReceptorListener>()));
    events = new ConcurrentLinkedQueue<F2SEvent>();
    keepRunning = new AtomicBoolean(false);
    nextConnectionTime = new AtomicReference<Instant>(new Instant(0));
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getConnectionParameters()
   */
  public KryoNetConnectionParameters getConnectionParameters() {
    return connectionParams;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getStats()
   */
  public EventReceptorStats getStats() {
    return stats;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents(int)
   */
  public List<F2SEvent> getEvents(final int maxEvents) {
    final List<F2SEvent> result = new ArrayList<F2SEvent>();
    for (int i = 0; i < maxEvents; i++) {
      final F2SEvent e = events.poll();
      if (e == null) {
        break;
      }
      stats.decrEventsQueued();
      result.add(e);
    }
    LOG.trace("Collected {} event(s) (max {})", result.size(), maxEvents);
    return result;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents()
   */
  public List<F2SEvent> getEvents() {
    return getEvents(Integer.MAX_VALUE);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#start()
   */
  public boolean start() {
    if (keepRunning.get()) {
      LOG.warn("KryoNet client of {} already started", connectionParams.getConnectionStr());
      return true;
    }
    LOG.trace("Starting KryoNet client of {}", connectionParams.getConnectionStr());
    keepRunning.set(true);
    stats.reset();
    nextConnectionTime.set(new Instant(0));
    maintainConnectionThread = new MaintainConnection();
    maintainConnectionThread.start();
    LOG.debug("KryoNet client of {} started", connectionParams.getConnectionStr());
    return true;
  }

  /**
   * Creates the KryoNet client and attempts to connect to the KryoNet server
   * 
   * @return True if connected successfully
   */
  protected boolean connect() {
    if (client != null)
      cleanup();
    LOG.debug("KryoNet client of {} connecting...", connectionParams.getConnectionStr());
    client = new Client(connectionParams.getWriteBufferSize(), connectionParams.getObjectBufferSize(),
        new MyKryoSerialization(connectionParams.getObjectBufferSize()));
    KryoUtil.register(client.getKryo());
    client.addListener(new KryoConnectionListener());
    client.start();
    try {
      client.connect(kryoNetParameters.getConnectionTimeout(), connectionParams.getAddress(),
          connectionParams.getPort());
      return true;
    } catch (final IOException e) {
      LOG.error("Failed to connect to the KryoNet server " + connectionParams.getConnectionStr(), e);
      return false;
    }
  }

  /**
   * @return True if connected, false otherwise
   */
  public boolean isConnected() {
    return client != null && client.isConnected();
  }

  /**
   * Cleans up the connection so it can try and reconnect
   */
  protected void cleanup() {
    try {
      LOG.debug("KryoNet client of {} closing...", connectionParams.getConnectionStr());
      if (client != null) {
        client.close();
        LOG.info("KryoNet client of {} closed", connectionParams.getConnectionStr());
      }
    } catch (Exception e) {
      LOG.error("Failed to close KryoNet client", e);
    } finally {
      client = null;
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#stop()
   */
  public boolean stop() {
    if (!keepRunning.get()) {
      LOG.warn("KryoNet client of {} already stopped", connectionParams.getConnectionStr());
      return true;
    }
    keepRunning.set(false);
    try {
      if (maintainConnectionThread == null) {
        maintainConnectionThread.join(kryoNetParameters.getTerminationTimeout());
      }
      return true;
    } catch (final InterruptedException e) {
      LOG.warn("Failed to terminate inner thread in {} ms", kryoNetParameters.getTerminationTimeout());
      return false;
    } finally {
      maintainConnectionThread = null;
    }
  }

  protected void setNextReconnectionTime() {
    nextConnectionTime.set(Instant.now().plus(kryoNetParameters.getRetrySleepDelay()));
  }

  protected class KryoConnectionListener extends Listener {
    public KryoConnectionListener() {
    }

    /**
     * @see com.esotericsoftware.kryonet.Listener#connected(com.esotericsoftware.kryonet.Connection)
     */
    @Override
    public void connected(final Connection connection) {
      stats.setConnected();
      LOG.info("KryoNet client of {} connected", connectionParams.getConnectionStr());
      for (EventReceptorListener erListener : listeners.get()) {
        erListener.onConnection();
      }
    }

    /**
     * @see com.esotericsoftware.kryonet.Listener#disconnected(com.esotericsoftware.kryonet.Connection)
     */
    @Override
    public void disconnected(final Connection connection) {
      stats.setDisconnected();
      LOG.info("KryoNet client of {} disconnected", connectionParams.getConnectionStr());
      cleanup();
      for (EventReceptorListener erListener : listeners.get()) {
        erListener.onDisconnection();
      }
    }

    /**
     * @see com.esotericsoftware.kryonet.Listener#received(com.esotericsoftware.kryonet.Connection,
     *      java.lang.Object)
     */
    @Override
    public void received(final Connection connection, final Object object) {
      if (object instanceof F2SEvent) {
        try {
          final F2SEvent fe = (F2SEvent) object;
          assert fe != null;
          LOG.trace("KryoNet client of {} received event {}", connectionParams.getConnectionStr(), fe);
          stats.incrEventsIn();
          if (!events.add(fe)) {
            LOG.warn("KryoNet client of {} failed to enqueue event {}", connectionParams.getConnectionStr(), fe);
          } else {
            stats.incrEventsQueued();
          }
          for (EventReceptorListener erListener : listeners.get()) {
            erListener.onEvent(ImmutableList.of(fe));
          }
        } catch (final ClassCastException cce) {
          LOG.error("KryoNet client of {} received a message of unexpected type: {}",
              connectionParams.getConnectionStr(), object.getClass());
        } catch (final Exception e) {
          LOG.error("KryoNet client of {} failed to handle message: {}", object);
        }
        setNextReconnectionTime();
      } else if (object instanceof KeepAlive) {
        // ignoring keep alive
      } else {
        LOG.trace("KryoNet client of {} ignoring unknown message of type {}", connectionParams.getConnectionStr(),
            object == null ? "null" : object.getClass());
      }
    }
  }

  public synchronized void addListener(EventReceptorListener listener) {
    List<EventReceptorListener> newList = new ArrayList<>(listeners.get());
    newList.add(listener);
    listeners.set(ImmutableList.copyOf(newList));
  }

  public synchronized void removeListener(EventReceptorListener listener) {
    List<EventReceptorListener> newList = new ArrayList<>(listeners.get());
    newList.remove(listener);
    listeners.set(ImmutableList.copyOf(newList));
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append(connectionParams)
        .append(kryoNetParameters).append(stats).append("nextConnectionTime", nextConnectionTime).toString();
  }
}
