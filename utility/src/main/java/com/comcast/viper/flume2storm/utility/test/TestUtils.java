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


/**
 * Some utility functions for test cases
 */
public class TestUtils {
  private static final int DEFAULT_RETRY_TIMEOUT = 100;

  /**
   * Waits that the condition is fulfilled - careful, this may never return!
   * Note that this method does not return a value because the condition is
   * obviously fulfilled
   * 
   * @param condition
   *          The condition to evaluate. It should be fast to evaluate
   * @throws InterruptedException
   *           If wait is interrupted
   */
  public static void waitFor(final TestCondition condition) throws InterruptedException {
    waitFor(condition, Integer.MAX_VALUE);
  }

  public static boolean waitFor(final TestCondition condition, final int maxWaitInMs) throws InterruptedException {
    return waitFor(condition, maxWaitInMs, DEFAULT_RETRY_TIMEOUT);
  }

  /**
   * Waits that the condition is fulfilled, for up to a specified amount of time
   * 
   * @param condition
   *          The condition to evaluate. It should be fast to evaluate
   * @param maxWaitInMs
   *          The maximum time to
   * @param retryTimeout
   *          The number of milliseconds to wait before retry the test
   * @return The evaluation of the {@link TestCondition} when done waiting;
   *         either because the condition is fulfilled (in which case the result
   *         is probably true), or because the timeout occurred (in which case
   *         the result is probably false).
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
