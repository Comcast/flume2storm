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
package com.comcast.viper.flume2storm.connection.sender;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.KryoUtil;
import com.comcast.viper.flume2storm.connection.KryoNetParameters;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.KryoNetServiceProvider;
import com.comcast.viper.flume2storm.utility.circular.CircularList;
import com.comcast.viper.flume2storm.utility.circular.ReadWriteCircularList;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;

/**
 * A Flume2Storm event sender implementation using KryoNet framework
 */
public class KryoNetEventSender extends KryoNetServiceProvider implements EventSender<KryoNetConnectionParameters> {
  private static final long serialVersionUID = 244675019786272856L;
  protected static final Logger LOG = LoggerFactory.getLogger(KryoNetEventSender.class);
  protected final EventSenderStats stats;
  protected final AtomicReference<KryoNetEventSenderStatus> status;
  protected final KryoNetParameters kryoNetParameters;
  protected final CircularList<Connection> clients;
  protected final Listener clientListener;
  protected Server server;
  protected KryoNetEventSenderStrategy senderStrategy;

  /**
   * Contructor
   * 
   * @param connectionParameters
   *          The {@link ConnectionParameters}
   * @param kryoNetParams
   *          KryoNet specific parameters
   */
  public KryoNetEventSender(KryoNetConnectionParameters connectionParameters, KryoNetParameters kryoNetParams) {
    super(connectionParameters);
    this.kryoNetParameters = kryoNetParams;
    stats = new EventSenderStats(connectionParameters.getId());
    status = new AtomicReference<KryoNetEventSenderStatus>(KryoNetEventSenderStatus.STOPPED);
    clients = new ReadWriteCircularList<Connection>();
    clientListener = new Listener.ThreadedListener(new Listener() {
      /**
       * @see com.esotericsoftware.kryonet.Listener#connected(com.esotericsoftware.kryonet.Connection)
       */
      @Override
      public void connected(final Connection connection) {
        LOG.debug("Connection (id: {}) from {}", connection.getID(), connection.getRemoteAddressTCP());
        clients.add(connection);
        stats.incrClients();
      }

      /**
       * @see com.esotericsoftware.kryonet.Listener#disconnected(com.esotericsoftware.kryonet.Connection)
       */
      @Override
      public void disconnected(final Connection connection) {
        LOG.debug("Disconnection (id: {})", connection.getID());
        clients.remove(connection);
        stats.decrClients();
      }
    });
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#getStats()
   */
  public EventSenderStats getStats() {
    return stats;
  }

  /**
   * @return The list of clients
   */
  protected CircularList<Connection> getClients() {
    return clients;
  }

  /**
   * @return The status of this {@link EventSender}
   */
  public KryoNetEventSenderStatus getStatus() {
    return status.get();
  }

  protected void setStatus(KryoNetEventSenderStatus newStatus) {
    assert newStatus != null;
    status.set(newStatus);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#start()
   */
  public synchronized boolean start() {
    if (getStatus().isOn()) {
      LOG.warn("Already started");
      return true;
    }
    LOG.trace("Starting KryoNet event distributor...");
    setStatus(KryoNetEventSenderStatus.STARTING);
    senderStrategy = new KryoNetSimpleRealtimeStrategy(this, kryoNetParameters.getMaxRetries(),
        connectionParameters.getWriteBufferSize(), connectionParameters.getObjectBufferSize());
    stats.reset();
    try {
      LOG.debug("Creating Kryo server with connection parameters: {}", connectionParameters);
      server = new Server(connectionParameters.getWriteBufferSize(), connectionParameters.getObjectBufferSize());
      KryoUtil.register(server.getKryo());
      server.addListener(clientListener);
      LOG.trace("Binding Kryo server...");
      server.bind(connectionParameters.getPort());
      LOG.debug("Starting Kryo server...");
      server.start();
      LOG.info("Kryo Flume server started successfully with {}", connectionParameters);
      setStatus(KryoNetEventSenderStatus.STARTED);
      return true;
    } catch (final Exception e) {
      LOG.error("Failed to start server with " + connectionParameters, e);
      stop();
      return false;
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#stop()
   */
  public synchronized boolean stop() {
    if (!getStatus().isOn()) {
      LOG.warn("Already stopped");
      return true;
    }
    boolean result = true;
    try {
      LOG.debug("Stopping KryoNet server...");
      server.stop();
    } catch (final Exception e) {
      LOG.error("Failed to stop KryoNet server", e);
      result = false;
    }
    setStatus(KryoNetEventSenderStatus.STOPPED);
    return result;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#send(java.util.List)
   */
  @Override
  public int send(List<F2SEvent> events) {
    if (!getStatus().isOn()) {
      LOG.error("Event sender cannot send event if it is not started");
      return 0;
    }
    if (clients.isEmpty())
      return 0;
    return senderStrategy.send(events);
  }
}