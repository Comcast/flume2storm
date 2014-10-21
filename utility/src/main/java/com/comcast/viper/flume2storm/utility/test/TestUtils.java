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

import java.net.ServerSocket;
import java.util.Random;

import com.google.common.base.Preconditions;

/**
 * Some utility functions for test cases
 */
public class TestUtils {
  private static final int DEFAULT_RETRY_TIMEOUT = 100;
  private static Random random = new Random();

  /**
   * @return An ephemeral port available for test usage
   */
  public static int getAvailablePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (Exception e) {
      throw new AssertionError("Failed to find available port", e);
    }
  }

  /**
   * @param n
   *          Max int allowed
   * @return A random number between 1 and n
   */
  public static final int getRandomPositiveInt(int n) {
    Preconditions.checkArgument(n > 1, "Cannot generate this kind of number!");
    return random.nextInt(n - 1) + 1;
  }

  /**
   * Waits that the condition is fulfilled - careful, this may never return!
   * 
   * @param condition
   *          The condition to evaluate. It should be fast to evaluate
   * @throws InterruptedException
   *           If wait is interrupted
   */
  public static void waitFor(final TestCondition condition) throws InterruptedException {
    waitFor(condition, Integer.MAX_VALUE);
  }

  /**
   * Waits that the condition is fulfilled, for up to a specified amount of time
   * 
   * @param condition
   *          The condition to evaluate. It should be fast to evaluate
   * @param maxWaitInMs
   *          The maximum number of milliseconds to wait
   * @return True if the condition is fulfilled in time, false otherwise
   * @throws InterruptedException
   *           If wait is interrupted
   */
  public static boolean waitFor(final TestCondition condition, final int maxWaitInMs) throws InterruptedException {
    return waitFor(condition, maxWaitInMs, DEFAULT_RETRY_TIMEOUT);
  }

  /**
   * Waits that the condition is fulfilled, for up to a specified amount of time
   * 
   * @param condition
   *          The condition to evaluate. It should be fast to evaluate
   * @param maxWaitInMs
   *          The maximum number of milliseconds to wait
   * @param retryTimeout
   *          The number of milliseconds to wait before retry the test
   * @return True if the condition is fulfilled in time, false otherwise
   * @throws InterruptedException
   *           If wait is interrupted
   */
  public static boolean waitFor(final TestCondition condition, final int maxWaitInMs, final int retryTimeout)
      throws InterruptedException {
    final long t0 = System.currentTimeMillis();
    while (!condition.evaluate() && (System.currentTimeMillis() - t0) < maxWaitInMs) {
      Thread.sleep(retryTimeout);
    }
    if (condition.evaluate()) {
      return true;
    }
    throw new AssertionError("TestCondition did not validate in time");
  }
}
