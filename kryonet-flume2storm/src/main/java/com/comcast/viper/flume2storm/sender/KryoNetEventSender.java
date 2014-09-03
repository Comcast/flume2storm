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
package com.comcast.viper.flume2storm.sender;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.KryoNetEventUtil;
import com.comcast.viper.flume2storm.KryoNetParameters;
import com.comcast.viper.flume2storm.connection.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
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
  protected final KryoNetEventSenderStats stats;
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
    stats = new KryoNetEventSenderStats();
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
   * @return Statistics related to this {@link EventSender}
   */
  public KryoNetEventSenderStats getStats() {
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
    return stats.getStatus();
  }

  protected void setStatus(KryoNetEventSenderStatus newStatus) {
    assert newStatus != null;
    stats.setStatus(newStatus);
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
    senderStrategy = new KryoNetSimpleRealtimeStrategy(this, kryoNetParameters.getMaxRetries());
    stats.reset();
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("Creating Kryo server on port {} with writeBufferSize={} and objectBufferSize={}...",
            new Object[] { connectionParameters.getServerPort(), connectionParameters.getWriteBufferSize(),
                connectionParameters.getObjectBufferSize() });
      server = new Server(connectionParameters.getWriteBufferSize(), connectionParameters.getObjectBufferSize());
      KryoNetEventUtil.register(server);
      server.addListener(clientListener);
      LOG.trace("Binding Kryo server...");
      server.bind(connectionParameters.getServerPort());
      LOG.debug("Starting Kryo server...");
      server.start();
      LOG.info("Kryo Flume server started succesfully");
      setStatus(KryoNetEventSenderStatus.STARTED);
      return true;
    } catch (final Exception e) {
      LOG.error("Failed to start server", e);
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
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#getNbReceptors()
   */
  @Override
  public int getNbReceptors() {
    return clients.size();
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