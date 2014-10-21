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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * A ZooKeeper test server, for test purpose
 */
public class ZkTestServer {
  protected static final Logger LOG = LoggerFactory.getLogger(ZkTestServer.class);
  protected static final File ZK_DIR = new File("src/test/zkTestData");
  protected static final int TICKTIME_DEFAULT = 500;

  protected final Integer port;
  protected final Integer ticktime;
  protected ServerCnxnFactory cnxnFactory;
  protected ZooKeeperServer zkServer;
  protected volatile boolean started;

  /**
   * Create a ZooKeeper test server using a random port available
   * 
   * @throws Exception
   *           errors
   */
  public ZkTestServer() {
    this(TestUtils.getAvailablePort(), TICKTIME_DEFAULT);
  }

  /**
   * Create a ZooKeeper test server using the specified port
   * 
   * @param port
   *          the port to use
   */
  public ZkTestServer(final int port) {
    this(port, TICKTIME_DEFAULT);
  }

  /**
   * Create a ZooKeeper test server using the specified parameters
   * 
   * @param port
   *          the port to use
   * @param ticktime
   *          The server tick time
   */
  public ZkTestServer(final int port, final int ticktime) {
    this.port = port;
    this.ticktime = ticktime;
    started = false;
    cleanup();
    LOG.debug("Created ZooKeeper test server on port {}", port);
  }

  /**
   * Cleans up the local file system - This erases all data stored by the server
   */
  public void cleanup() {
    try {
      LOG.debug("Removing local directory...");
      FileUtils.deleteDirectory(ZK_DIR);
      LOG.debug("Removed local directory");
    } catch (IOException e) {
      LOG.error("Failed to remove local directory: " + e.getMessage(), e);
    }
  }

  /**
   * Starts the test ZooKeeper server
   */
  public void start() {
    if (started) {
      LOG.debug("Already started");
      return;
    }
    try {
      LOG.debug("Starting...");
      ServerConfig config = new ServerConfig();
      config.parse(new String[] { port.toString(), ZK_DIR.getCanonicalPath(), ticktime.toString() });

      zkServer = new ZooKeeperServer();

      FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));
      zkServer.setTxnLogFactory(ftxn);
      zkServer.setTickTime(config.getTickTime());
      zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
      zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());

      cnxnFactory = ServerCnxnFactory.createFactory();
      cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
      cnxnFactory.startup(zkServer);
      started = true;
      LOG.info("Started, {}", getConnectString());
    } catch (Exception e) {
      LOG.error("Failed to start: " + e.getMessage(), e);
    }
  }

  /**
   * Stops the test ZooKeeper server
   */
  public void stop() {
    if (!started) {
      LOG.debug("Not started");
      return;
    }
    try {
      LOG.info("stopping....");
      cnxnFactory.shutdown();
      cnxnFactory.join();
      if (zkServer.isRunning()) {
        zkServer.shutdown();
      }
      started = false;
      LOG.info("Stopped");
    } catch (Exception e) {
      LOG.error("Failed to stop: " + e.getMessage(), e);
    }
  }

  /**
   * Return the port being used
   * 
   * @return port
   */
  public int getPort() {
    return port;
  }

  /**
   * Returns the connection string to use
   * 
   * @return connection string
   */
  public String getConnectString() {
    return "localhost:" + port;
  }

  /**
   * @return ZooKeeper's server ticktime
   */
  protected int getTicktime() {
    return ticktime;
  }
}