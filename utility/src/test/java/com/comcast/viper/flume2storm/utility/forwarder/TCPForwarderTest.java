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
package com.comcast.viper.flume2storm.utility.forwarder;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Test {@link TCPForwarder} class
 */
public class TCPForwarderTest {
  private static final int TRANSITION_SLEEP_TIME = 200;

  /**
   * Test {@link TCPForwarder} class
   * 
   * @throws InterruptedException
   *           If interrupted
   */
  @Test
  public void testIt() throws InterruptedException {
    final TCPForwarderBuilder tcpForwarderBuilder = new TCPForwarderBuilder();
    tcpForwarderBuilder.setInputPort(1111).setOutputPort(2222).setOutputServer("127.0.0.1");
    final TCPForwarder tcpForwarder = tcpForwarderBuilder.build();
    Assert.assertFalse(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.start();
    tcpForwarder.start();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertTrue(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.start();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertTrue(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.freeze();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertFalse(tcpForwarder.isActive());
    Assert.assertTrue(tcpForwarder.isFrozen());
    tcpForwarder.freeze();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertFalse(tcpForwarder.isActive());
    Assert.assertTrue(tcpForwarder.isFrozen());
    tcpForwarder.resume();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertTrue(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.resume();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertTrue(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.stop();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    tcpForwarder.start();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertTrue(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
    tcpForwarder.freeze();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertFalse(tcpForwarder.isActive());
    Assert.assertTrue(tcpForwarder.isFrozen());
    tcpForwarder.stop();
    Thread.sleep(TRANSITION_SLEEP_TIME);
    Assert.assertFalse(tcpForwarder.isActive());
    Assert.assertFalse(tcpForwarder.isFrozen());
  }
}
