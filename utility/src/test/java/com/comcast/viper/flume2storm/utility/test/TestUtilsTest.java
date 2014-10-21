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
package com.comcast.viper.flume2storm.utility.test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.utility.Constants;

/**
 * Test {@link TestUtils}
 */
public class TestUtilsTest {
  protected static final Logger LOG = LoggerFactory.getLogger(TestUtilsTest.class);
  private static final int DURATION = 800;
  protected final AtomicBoolean threadDone = new AtomicBoolean();

  /**
   * Test initialization
   */
  @Before
  public void init() {
    threadDone.set(false);
  }

  protected class TestThread extends Thread {
    @Override
    public void run() {
      LOG.debug("Starting thread...");
      try {
        Thread.sleep(DURATION);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
      LOG.debug("Triggering condition!");
      threadDone.set(true);
      LOG.debug("Thread terminated");
    }
  }

  protected class MyTestCondition implements TestCondition {
    @Override
    public boolean evaluate() {
      LOG.debug("Evaluating...");
      return threadDone.get();
    }
  }

  /**
   * Test {@link TestUtils#waitFor(TestCondition)}
   * 
   * @throws InterruptedException
   *           If interrupted
   */
  @Test
  public void testInfinite() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.debug("Waiting for condition (thread terminated)...");
    TestUtils.waitFor(new MyTestCondition());
    LOG.debug("Done waiting");
    thread.join();
  }

  /**
   * Test {@link TestUtils#waitFor(TestCondition, int)} failure
   * 
   * @throws InterruptedException
   *           If interrupted
   */
  @Test(expected = AssertionError.class)
  public void testTimeoutFail() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.debug("Waiting for condition (thread terminated)...");
    TestUtils.waitFor(new MyTestCondition(), DURATION / 2);
  }

  /**
   * Test {@link TestUtils#waitFor(TestCondition, int)} success
   * 
   * @throws InterruptedException
   *           If interrupted
   */
  @Test
  public void testTimeoutSuccess() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.debug("Waiting for condition (thread terminated)...");
    final boolean result = TestUtils.waitFor(new MyTestCondition(), DURATION * 2);
    LOG.debug("Done waiting");
    Assert.assertTrue(result);
    thread.join();
  }

  /**
   * Test {@link TestUtils#getAvailablePort()}
   */
  @Test
  public void testGetAvailablePort() {
    int port1 = TestUtils.getAvailablePort();
    Assert.assertTrue(port1 > 0);
    Assert.assertTrue(port1 < Constants.MAX_UNSIGNED_SHORT);
    int port2 = TestUtils.getAvailablePort();
    Assert.assertTrue(port2 > 0);
    Assert.assertTrue(port2 < Constants.MAX_UNSIGNED_SHORT);
    Assert.assertTrue(port1 != port2);
  }
}
