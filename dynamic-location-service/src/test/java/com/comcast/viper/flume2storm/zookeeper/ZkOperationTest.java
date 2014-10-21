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

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class also demonstrate the use of {@link ZkOperation} construction
 */
public class ZkOperationTest extends ZkClientTestUtils {
  protected static final Logger LOG = LoggerFactory.getLogger(ZkOperationTest.class);

  private ZkTestServer zkServer;

  @Before
  public void setup() {
    zkServer = new ZkTestServer();
    zkServer.start();
  }

  @After
  public void tearDown() {
    zkServer.stop();
    zkServer.cleanup();
  }

  // TODO test chroot
  @Ignore
  @Test
  public void testNodeManagement() throws Exception {
    final String node1path = "/gab";
    final byte[] node1data = "gab".getBytes();

    LOG.info("### Test step: Creating client...");
    zkClient = new ZkClient(null);
    zkClient.configure(ZkClientTestUtils.getZkClientConfig(zkServer));
    zkClient.start();
    waitZkClientSetup();

    LOG.info("### Test step: Creating node...");
    // New ZkOperation using the default constructor
    Assert.assertFalse(new ZkOperation().with(zkClient).path(node1path).nodeExists());
    // New ZkOperation using the constructor with arguments
    final ZkOperation zkOp = new ZkOperation(zkClient, node1path);
    Assert.assertEquals(
        node1path,
        zkOp.createNode(new ZkNodeCreationArg().setData(node1data).setCreateMode(CreateMode.PERSISTENT)
            .setCreateHierarchy(false)));
    Assert.assertTrue(new ZkOperation(zkClient, node1path).nodeExists());
    Assert.assertArrayEquals(node1data, new ZkOperation(zkClient, node1path).getData());

    LOG.info("### Test step: Deleting node...");
    // New ZkOperation using the static builder
    ZkOperation.get().with(zkClient).path(node1path).deleteNode();
    Assert.assertFalse(zkOp.path(node1path).nodeExists());

    LOG.info("### Test step: Recursive node creation...");
    final String deepPath = "/me/and/my/friend";
    final byte[] deepData = "Deep Purple".getBytes();
    Assert.assertFalse(new ZkOperation(zkClient, deepPath).nodeExists());
    Assert.assertFalse(new ZkOperation(zkClient, "/me/and/my").nodeExists());
    Assert.assertFalse(new ZkOperation(zkClient, "/me/and").nodeExists());
    Assert.assertFalse(new ZkOperation(zkClient, "/me").nodeExists());
    Assert.assertEquals(
        deepPath,
        zkOp.path(deepPath).createNode(
            new ZkNodeCreationArg().setData(deepData).setCreateHierarchy(true).setCreateMode(CreateMode.EPHEMERAL)));
    Assert.assertTrue(new ZkOperation(zkClient, "/me").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me").getData());
    Assert.assertTrue(new ZkOperation(zkClient, "/me/and").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me/and").getData());
    Assert.assertTrue(new ZkOperation(zkClient, "/me/and/my").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me/and/my").getData());
    Assert.assertTrue(new ZkOperation(zkClient, deepPath).nodeExists());
    Assert.assertArrayEquals(deepData, new ZkOperation(zkClient, deepPath).getData());
    // And again:
    Assert.assertEquals(
        deepPath,
        zkOp.path(deepPath).createNode(
            new ZkNodeCreationArg().setCreateHierarchy(true).setCreateMode(CreateMode.EPHEMERAL)));
    Assert.assertTrue(new ZkOperation(zkClient, "/me").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me").getData());
    Assert.assertTrue(new ZkOperation(zkClient, "/me/and").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me/and").getData());
    Assert.assertTrue(new ZkOperation(zkClient, "/me/and/my").nodeExists());
    Assert.assertArrayEquals(new byte[0], new ZkOperation(zkClient, "/me/and/my").getData());
    Assert.assertTrue(new ZkOperation(zkClient, deepPath).nodeExists());
    Assert.assertArrayEquals(deepData, new ZkOperation(zkClient, deepPath).getData());

    LOG.info("### Test step: Recursive node deletion...");
    try {
      ZkOperation.get().with(zkClient).path("/me").deleteNode();
      Assert.fail("Should raise an exception!");
    } catch (final Exception e) {
      e.printStackTrace();
    }
    Assert.assertTrue(new ZkOperation(zkClient, "/me").nodeExists());
    ZkOperation.get().with(zkClient).path("/me").deleteRecursive();
    Assert.assertFalse(new ZkOperation(zkClient, "/me").nodeExists());

    zkClient.stop();
  }
}
