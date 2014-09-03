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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.utility.forwarder.TCPForwarder;
import com.comcast.viper.flume2storm.utility.forwarder.TCPForwarderBuilder;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * This tests the state machine of the {@link ZkClient}. We use a listener in
 * order to test the recovery mechanism, through the creation of an ephemeral
 * node on init
 * 
 * @author gcommeau
 */
public class ZkClientStateMachineTest extends ZkClientTestUtils {
	protected static final Logger LOG = LoggerFactory.getLogger(ZkClientStateMachineTest.class);
	protected static final int FORWARDER_PORT = 1234;

	private MyZkClientListener listener;
	private TCPForwarder forwarder;

	protected static class MyZkClientListener implements ZkClientListener {
		protected static final Logger LOG = LoggerFactory.getLogger(MyZkClientListener.class);
		protected static final String NODE_PATH = "/test";
		protected static final byte[] NODE_DATA = "test".getBytes();

		protected final AtomicInteger initialization = new AtomicInteger(0);
		protected final AtomicInteger temrination = new AtomicInteger(0);
		protected final AtomicInteger connection = new AtomicInteger(0);
		protected final AtomicInteger disconnection = new AtomicInteger(0);
		protected final AtomicBoolean nodeExitsOnConnection = new AtomicBoolean(false);

		private static ZkOperation zkOps() {
			return new ZkOperation(zkClient, NODE_PATH);
		}

		@Override
		public void initialize() {
			LOG.debug("initialize!");
			initialization.incrementAndGet();
			// Creating node on init, but only if it's not already there
			if (!nodeExitsOnConnection.get()) {
				try {
					LOG.debug("Creating ephemeral node");
					zkOps().createNode(new ZkNodeCreationArg().setData(NODE_DATA).setCreateMode(CreateMode.EPHEMERAL));
				} catch (final Exception e) {
					Assert.fail(e.getMessage());
				}
			}
		}

		@Override
		public void terminate() {
			LOG.debug("terminate!");
			temrination.incrementAndGet();
			try {
				LOG.debug("Deleting ephemeral node");
				zkOps().deleteNode();
			} catch (final Exception e) {
				Assert.fail(e.getMessage());
			}
		}

		@Override
		public void onConnection() {
			LOG.debug("onConnection!");
			connection.incrementAndGet();
			// testing if node was created on connection
			try {
				final boolean nodeExists = zkOps().nodeExists();
				LOG.debug("On connection, node {} exists returns {}", NODE_PATH, nodeExists);
				nodeExitsOnConnection.set(nodeExists);
			} catch (final Exception e) {
				Assert.fail(e.getMessage());
			}
		}

		@Override
		public void onDisconnection() {
			LOG.debug("onDisconnection!");
			disconnection.incrementAndGet();
		}

		public void assertStatus(final int nbConnection, final int nbInitialization, final int nbTermination, final int nbDisconnection,
				final boolean checkNodeExists, final Boolean nodeExistsOnConnection) throws Exception {
			Assert.assertEquals(nbConnection, connection.get());
			Assert.assertEquals(nbInitialization, initialization.get());
			Assert.assertEquals(nbTermination, temrination.get());
			Assert.assertEquals(nbDisconnection, disconnection.get());
			if (checkNodeExists) {
				Assert.assertTrue(zkOps().nodeExists());
			}
			if (nodeExistsOnConnection != null) {
				Assert.assertEquals(nodeExistsOnConnection.booleanValue(), this.nodeExitsOnConnection.get());
			}
		}
	}

	@Before
	public void init() throws Exception {
		final TCPForwarderBuilder forwarderBuilder = new TCPForwarderBuilder();
		forwarderBuilder.setListenAddress(HOST).setInputPort(FORWARDER_PORT).setOutputServer(HOST).setOutputPort(PORT);
		forwarder = forwarderBuilder.build();
		forwarder.start();
		while (!forwarder.isActive()) {
			Thread.sleep(100);
		}

		listener = new MyZkClientListener();
		zkClient = new ZkClient(listener);
		// Programming note: setting the session timeout to 2 tick times (small
		// but not too much)
		ZK_CONFIGURATION.setConnectionStr(HOST + ":" + FORWARDER_PORT);
		ZK_CONFIGURATION.setSessionTimeout(2 * ZkServerTestUtils.TICK_TIME);
		zkClient.configure(ZK_CONFIGURATION);
		assertZkClientStopped();
		listener.assertStatus(0, 0, 0, 0, false, null);
	}

	@After
	public void terminate() throws Exception {
		forwarder.stop();
	}

	// TODO test invalid server? (disconnected after connecting)

	@Test
	public void testClientStates() throws Exception {
		LOG.info("\n###\n### Test step: Starting client (no Zk server)...");
		Assert.assertTrue(zkClient.start());
		waitZkClientStarted();
		listener.assertStatus(0, 0, 0, 0, false, null);

		LOG.info("\n###\n### Test step: Starting client twice...");
		Assert.assertFalse(zkClient.start());
		assertZkClientStarted();
		listener.assertStatus(0, 0, 0, 0, false, null);
		Thread.sleep(A_LITTLE);

		LOG.info("\n###\n### Test step: Stopping client...");
		Assert.assertTrue(zkClient.stop());
		assertZkClientStopped();
		listener.assertStatus(0, 0, 0, 0, false, null);

		LOG.info("\n###\n### Test step: Stopping client twice...");
		Assert.assertFalse(zkClient.stop());
		assertZkClientStopped();
		listener.assertStatus(0, 0, 0, 0, false, null);

		LOG.info("\n###\n### Test step: Restarting client (still no Zk server)...");
		Assert.assertTrue(zkClient.start());
		waitZkClientStarted();
		listener.assertStatus(0, 0, 0, 0, false, null);

		LOG.info("\n###\n### Test step: Starting ZK...");
		ZkServerTestUtils.startZkServer();
		ZkServerTestUtils.waitZkServerOn();

		LOG.info("\n###\n### Test step: Waiting that the ZkClient connects the server...");
		waitZkClientConnected();
		waitZkClientSetup(60000);
		listener.assertStatus(1, 1, 0, 0, true, false);
		Thread.sleep(A_LITTLE);

		LOG.info("\n###\n### Test step: Stopping client...");
		Assert.assertTrue(zkClient.stop());
		assertZkClientStopped();
		listener.assertStatus(1, 1, 1, 1, false, null);

		LOG.info("\n###\n### Test step: Restarting client...");
		Assert.assertTrue(zkClient.start());
		waitZkClientSetup();
		listener.assertStatus(2, 2, 1, 1, true, false);

		LOG.info("\n###\n### Test step: Stopping ZK Server for a small inturruption (< session timeout)...");
		ZkServerTestUtils.stopZkServer();
		ZkServerTestUtils.waitZkServerOff();
		waitZkClientDisconnected();
		// Programming note: The disconnection call back happens after the state
		// change and therefore, we need to wait a little extra
		Assert.assertTrue(TestUtils.waitFor(new TestCondition() {
			@Override
			public boolean evaluate() {
				return listener.disconnection.get() == 2;
			}
		}, TEST_TIMEOUT));
		listener.assertStatus(2, 2, 1, 2, false, null);
		Thread.sleep(A_LITTLE);

		LOG.info("\n###\n### Test step: Restarting ZK Server before session times out...");
		ZkServerTestUtils.startZkServer();
		ZkServerTestUtils.waitZkServerOn();
		waitZkClientSetup(TEST_TIMEOUT);
		listener.assertStatus(3, 2, 1, 2, true, true);
		// Recording session timeout
		final Integer sessionTO = zkClient.getNegotiatedSessionTimeout();
		Assert.assertTrue(sessionTO >= ZkServerTestUtils.TICK_TIME);

		LOG.info("\n###\n### Test step: Stopping ZK Server for a long inturruption (> session timeout)...");
		ZkServerTestUtils.stopZkServer();
		ZkServerTestUtils.waitZkServerOff();
		waitZkClientDisconnected();
		// Programming note: The disconnection call back happens after the state
		// change and therefore, we need to wait a little extra
		Assert.assertTrue(TestUtils.waitFor(new TestCondition() {
			@Override
			public boolean evaluate() {
				return listener.disconnection.get() == 3;
			}
		}, TEST_TIMEOUT));
		listener.assertStatus(3, 2, 1, 3, false, null);
		LOG.info("Waiting {} ms", sessionTO * 2);
		Thread.sleep(sessionTO * 2);

		LOG.info("\n###\n### Test step: Restarting ZK Server after session times out...");
		ZkServerTestUtils.startZkServer();
		ZkServerTestUtils.waitZkServerOn();
		waitZkClientSetup(TEST_TIMEOUT);
		listener.assertStatus(4, 2, 1, 3, true, true);

		LOG.info("\n###\n### Test step: Freezing proxy between client and server for a little while (< Session TO)...");
		forwarder.freeze();
		Thread.sleep(A_LITTLE);
		forwarder.resume();
		Assert.assertTrue(MyZkClientListener.zkOps().nodeExists());
		listener.assertStatus(4, 2, 1, 3, true, true);

		LOG.info("\n###\n### Test step: Freezing proxy between client and server for a longer while (> Session TO)...");
		forwarder.freeze();
		Thread.sleep(sessionTO * 2);
		assertZkClientStarted();
		listener.assertStatus(4, 2, 1, 4, false, null);
		forwarder.resume();
		waitZkClientSetup(TEST_TIMEOUT * 2);
		listener.assertStatus(5, 3, 1, 4, true, false);

		LOG.info("\n###\n### Test step: Terminating test...");
		zkClient.stop();
		ZkServerTestUtils.stopZkServer();
		ZkServerTestUtils.waitZkServerOff();
	}
}
