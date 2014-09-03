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

import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.netflix.curator.test.MyZkTestServer;

/**
 * 
 * 
 * @author gcommeau
 */
public class ZkServerTestUtils extends ZkTestUtils
{
	protected static final int TICK_TIME = 2000;
	protected static MyZkTestServer zkServer;

	public static void startZkServer() throws Exception {
		if (zkServer == null) {
			zkServer = new MyZkTestServer(PORT, TICK_TIME);
		} else {
			zkServer.start();
		}
	}

	public static void stopZkServer() {
		zkServer.stop();
	}

	public static void clearZkServer() {
		if (zkServer != null) {
			stopZkServer();
			zkServer = null;
		}
	}

	public static void waitZkServerOn() throws Exception {
		waitZkServerOn(ZK_OP_TIMEOUT);
	}

	public static void waitZkServerOn(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(ZK_IS_ON, ZK_OP_TIMEOUT));
	}

	public static void waitZkServerOff() throws Exception {
		waitZkServerOff(ZK_OP_TIMEOUT);
	}

	public static void waitZkServerOff(final int timeout) throws Exception {
		Assert.assertTrue(TestUtils.waitFor(ZK_IS_OFF, ZK_OP_TIMEOUT));
	}
}
