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
package com.comcast.viper.flume2storm.location;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkClient;
import com.comcast.viper.flume2storm.zookeeper.ZkClientListener;
import com.comcast.viper.flume2storm.zookeeper.ZkNodeCreationArg;
import com.comcast.viper.flume2storm.zookeeper.ZkOperation;
import com.comcast.viper.flume2storm.zookeeper.ZkUtilies;
import com.google.common.base.Preconditions;

/**
 * An implementation of LocationService that uses ZooKeeper for coordination:
 * the ServiceProviders advertise their presence by maintaining an ephemeral
 * znode.
 * 
 * @param <SP>
 *          The Service Provider class
 */
public class DynamicLocationService<SP extends ServiceProvider<?>> extends AbstractLocationService<SP> {
  // TODO It would be nice to have a utility that connects using the dynamic
  // location service and expose the service providers available
  protected static final Logger LOG = LoggerFactory.getLogger(DynamicLocationService.class);
  private static final String SNODE_BASE_NAME = "server";
  protected final ZkClient zkClient;
  // ZK's path for that service: $basePath/$serviceName
  protected final String servicePath;
  // Local registration
  private final Map<SP, String> registrations;
  private final ServiceProviderSerialization<SP> serviceProviderSerialization;

  /**
   * Constructor for {@link DynamicLocationService}
   * 
   * @param config
   *          The {@link DynamicLocationService} configuration
   * @param ser
   *          The {@link ServiceProvider} serialization
   */
  public DynamicLocationService(final DynamicLocationServiceConfiguration config,
      final ServiceProviderSerialization<SP> ser) {
    this.serviceProviderSerialization = ser;
    zkClient = new ZkClient(new ZkListener());
    zkClient.configure(config);
    servicePath = ZkUtilies.buildZkPath(config.getBasePath(), config.getServiceName());
    registrations = new ConcurrentHashMap<SP, String>();
    LOG.debug("Created DynamicLocationService with: {}", config);
  }

  /**
   * Connects ZooKeeper and initializes the {@link DynamicLocationService}
   * 
   * @see com.comcast.viper.flume2storm.location.LocationService#start()
   */
  @Override
  public boolean start() {
    LOG.debug("Starting...");
    if (!zkClient.start())
      return false;
    // TODO use configuration to set a max timeout
    return waitReady();
  }

  /**
   * Disconnects ZooKeeper and terminates the {@link DynamicLocationService}
   * 
   * @see com.comcast.viper.flume2storm.location.LocationService#stop()
   */
  @Override
  public boolean stop() {
    LOG.debug("Stopping...");
    return zkClient.stop();
  }

  /**
   * @return True if connected to Zookeeper
   */
  public boolean isConnected() {
    return zkClient.getState().isConnected();
  }

  private class ZkClientReadyCondition implements TestCondition {
    protected ZkClientReadyCondition() {
      super();
    }

    /**
     * @see com.comcast.viper.flume2storm.utility.test.TestCondition#evaluate()
     */
    @Override
    public boolean evaluate() {
      return zkClient.getState().isSetup();
    }
  }

  /**
   * Sleeps until the library is ready to proceed (i.e. connection established,
   * setup completed, ...)
   * 
   * @param timeout
   *          The maximum amount of time to wait (in milliseconds)
   * @return True if the {@link DynamicLocationService} is ready, false if the
   *         operation timed out
   */
  protected boolean waitReady(final int timeout) {
    try {
      return TestUtils.waitFor(new ZkClientReadyCondition(), timeout, 100);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Sleeps until the library is ready to proceed (i.e. connection established,
   * setup completed, ...)
   * 
   * @throws InterruptedException
   *           If the thread is interrupted while waiting
   */
  protected boolean waitReady() {
    return waitReady(Integer.MAX_VALUE);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#register(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void register(SP serviceProvider) {
    Preconditions.checkNotNull(serviceProvider);
    // TODO Perhaps put a request on the queue to make sure this happens
    // when connected
    if (registrations.containsKey(serviceProvider)) {
      LOG.warn("ServiceProvider {} already registered", serviceProvider);
      return;
    }
    if (!isConnected()) {
      LOG.warn("Not connected to ZooKeeper");
      return;
    }
    LOG.debug("Registering service...");
    try {
      // TODO improve disconnection performances by using a std
      // ephemeral node and using UUID of the ServiceInstance
      final byte[] bytes = serviceProviderSerialization.serialize(serviceProvider);
      final String p = ZkUtilies.buildZkPath(servicePath, SNODE_BASE_NAME);
      String lastNodeName = new ZkOperation(zkClient, p).createNode(new ZkNodeCreationArg().setCreateMode(
          CreateMode.EPHEMERAL_SEQUENTIAL).setData(bytes));
      LOG.debug("Created ephemeral node: {}", lastNodeName);
      registrations.put(serviceProvider, lastNodeName);
    } catch (final Exception e) {
      LOG.error("Failed to register server instance: " + serviceProvider, e);
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#unregister(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void unregister(SP serviceProvider) {
    Preconditions.checkNotNull(serviceProvider);
    if (!isConnected()) {
      LOG.warn("Not connected to ZooKeeper");
      return;
    }
    if (!registrations.containsKey(serviceProvider)) {
      LOG.warn("ServiceProvider {} not registered locally", serviceProvider);
      return;
    }
    try {
      new ZkOperation(zkClient, registrations.remove(serviceProvider)).deleteNode();
    } catch (Exception e) {
      LOG.error("Failed to unregister server instance: " + serviceProvider, e);
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#getSerialization()
   */
  public ServiceProviderSerialization<SP> getSerialization() {
    return serviceProviderSerialization;
  }

  private class ZkListener implements ZkClientListener, Serializable {
    private static final long serialVersionUID = 4468828482344119321L;

    protected ZkListener() {
      super();
    }

    /**
     * @see com.comcast.viper.flume2storm.zookeeper.ZkClientListener#initialize()
     */
    @Override
    public void initialize() {
      // Making directory structure
      try {
        LOG.debug("Enforcing base path ({}) existence...", servicePath);
        new ZkOperation(zkClient, servicePath).createNode(new ZkNodeCreationArg().setCreateHierarchy(true)
            .setCreateMode(CreateMode.PERSISTENT));
        getServiceInstances();
      } catch (final Exception e) {
        LOG.error("Failed to setup ZooKeeper connection", e);
      }
      // Registering
      // TODO if any?
    }

    /**
     * @see com.comcast.viper.flume2storm.zookeeper.ZkClientListener#terminate()
     */
    @Override
    public void terminate() {
      // Nothing to do
    }

    /**
     * @see com.comcast.viper.flume2storm.zookeeper.ZkClientListener#onConnection()
     */
    @Override
    public void onConnection() {
      // Nothing to do
    }

    /**
     * @see com.comcast.viper.flume2storm.zookeeper.ZkClientListener#onDisconnection()
     */
    @Override
    public void onDisconnection() {
      // Nothing to do
    }
  }

  /**
   * Updates the list of active {@link ServiceProvider} servers, and sets a
   * watch so that we get notified if the list changes
   */
  protected synchronized void getServiceInstances() {
    if (!zkClient.getState().isConnected()) {
      return;
    }
    final Watcher watcher = new Watcher() {
      @Override
      public void process(final WatchedEvent event) {
        LOG.debug("Service node watch triggered with event: {}", event);
        getServiceInstances();
      }
    };
    try {
      final List<String> res = new ZkOperation(zkClient, servicePath).getChildren(watcher);
      final Collection<SP> newList = new ArrayList<SP>();
      for (final String child : res) {
        final String childPath = ZkUtilies.buildZkPath(servicePath, child);
        try {
          final byte[] bytes = new ZkOperation(zkClient, childPath).getData();
          newList.add(serviceProviderSerialization.deserialize(bytes));
        } catch (final KeeperException.NoNodeException nne) {
          // Programming note: The node was removed been the time we
          // got the
          // list and the parsing time... no big deal
          LOG.debug("znode {} disappear: {}", childPath, nne.getLocalizedMessage());
        } catch (final Exception e) {
          LOG.warn("Failed to deserialize znode " + childPath, e);
        }
      }
      serviceProviderManager.set(newList);
    } catch (final Exception e) {
      if (zkClient.getState().isConnected()) {
        LOG.error("Failed to get service instances", e);
      }
      // Otherwise, we might have a clue why this is failing! :)
    }
  }
}
