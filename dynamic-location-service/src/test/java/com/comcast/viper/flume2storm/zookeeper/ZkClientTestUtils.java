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

import junit.framework.Assert;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * 
 * 
 * @author gcommeau
 */
public class ZkClientTestUtils extends ZkTestUtils {
	public static final int TEST_RECONNECTION_DELAY = 2000;
	public static final ZkClientConfiguration ZK_CONFIGURATION = new ZkClientConfiguration();
	static {
		ZK_CONFIGURATION.setConnectionStr(HOST + ":" + PORT);
		ZK_CONFIGURATION.setReconnectionDelay(TEST_RECONNECTION_DELAY);
	}

	protected static ZkClient zkClient;

	public static class ClientStartedCondition implements TestCondition {
		private final ZkClient zkClient;

		public ClientStartedCondition(final ZkClient zkClient) {
			this.zkClient = zkClient;
		}

		@Override
		public boolean evaluate() {
			return zkClient.getState().isStarted();
		}
	}

	public static class ClientConnectedCondition implements TestCondition {
		private final ZkClient zkClient;

		public ClientConnectedCondition(final ZkClient zkClient) {
			this.zkClient = zkClient;
		}

		@Override
		public boolean evaluate() {
			return zkClient.getState().isConnected();
		}
	}

	public static class ClientDisconnectedCondition implements TestCondition {
		private final ZkClient zkClient;

		public ClientDisconnectedCondition(final ZkClient zkClient) {
			this.zkClient = zkClient;
		}

		@Override
		public boolean evaluate() {
			return !zkClient.getState().isConnected();
		}
	}

	public static class ClientSetupCondition implements TestCondition {
		private final ZkClient zkClient;

		public ClientSetupCondition(final ZkClient zkClient) {
			this.zkClient = zkClient;
		}

		@Override
		public boolean evaluate() {
			return zkClient.getState().isSetup();
		}
	}

	public static void waitZkClientStarted(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(new ClientStartedCondition(zkClient), timeout));
		assertZkClientStarted();
	}

	public static void waitZkClientStarted() throws Exception {
		waitZkClientStarted(TEST_TIMEOUT);
	}

	public static void waitZkClientConnected(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(new ClientConnectedCondition(zkClient), timeout));
	}

	public static void waitZkClientConnected() throws Exception {
		waitZkClientConnected(TEST_TIMEOUT);
	}

	public static void waitZkClientDisconnected(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(new ClientDisconnectedCondition(zkClient), timeout));
		assertZkClientStarted();
	}

	public static void waitZkClientDisconnected() throws Exception {
		waitZkClientDisconnected(TEST_TIMEOUT);
	}

	public static void waitZkClientSetup(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(new ClientSetupCondition(zkClient), timeout));
		assertZkClientSetup();
	}

	public static void waitZkClientSetup() throws Exception {
		waitZkClientSetup(TEST_TIMEOUT);
	}

	public static void assertZkClientStopped() {
		Assert.assertFalse(zkClient.getState().isStarted());
		Assert.assertFalse(zkClient.getState().isConnected());
		Assert.assertFalse(zkClient.getState().isSetup());
	}

	public static void assertZkClientStarted() {
		Assert.assertTrue(zkClient.getState().isStarted());
		Assert.assertFalse(zkClient.getState().isConnected());
		Assert.assertFalse(zkClient.getState().isSetup());
	}

	public static void assertZkClientConnected() {
		Assert.assertTrue(zkClient.getState().isStarted());
		Assert.assertTrue(zkClient.getState().isConnected());
		Assert.assertFalse(zkClient.getState().isSetup());
	}

	public static void assertZkClientSetup() {
		Assert.assertTrue(zkClient.getState().isStarted());
		Assert.assertTrue(zkClient.getState().isConnected());
		Assert.assertTrue(zkClient.getState().isSetup());
	}

}
