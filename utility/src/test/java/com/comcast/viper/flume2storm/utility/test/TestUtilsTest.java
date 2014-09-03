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

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtilsTest {
  protected static final Logger LOG = LoggerFactory.getLogger(TestUtilsTest.class);
  protected final AtomicBoolean threadDone = new AtomicBoolean();

  @BeforeClass
  public static void initTest() {
    BasicConfigurator.configure();
  }

  @Before
  public void initTestCase() {
    threadDone.set(false);
  }

  private class TestThread extends Thread {
    @Override
    public void run() {
      LOG.info("Starting thread...");
      try {
        Thread.sleep(800);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Triggering condition!");
      threadDone.set(true);
      LOG.info("Thread terminated");
    }
  }

  private class MyTestCondition implements TestCondition {
    @Override
    public boolean evaluate() {
      LOG.info("Evaluating...");
      return threadDone.get();
    }
  }

  @Test
  public void testInfinite() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.info("Waiting for condition (thread terminated)...");
    TestUtils.waitFor(new MyTestCondition());
    LOG.info("Done waiting");
    thread.join();
  }

  @Test(expected = AssertionError.class)
  public void testTimeoutFail() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.info("Waiting for condition (thread terminated)...");
    final boolean result = TestUtils.waitFor(new MyTestCondition(), 400);
    LOG.info("Done waiting");
    thread.join();
  }

  @Test
  public void testTimeoutSuccess() throws InterruptedException {
    final Thread thread = new TestThread();
    thread.start();
    LOG.info("Waiting for condition (thread terminated)...");
    final boolean result = TestUtils.waitFor(new MyTestCondition(), 2000);
    LOG.info("Done waiting");
    Assert.assertTrue(result);
    thread.join();
  }
}
