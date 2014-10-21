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
package com.comcast.viper.flume2storm.connection.receptor;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * Utilities to facilitate testing of an {@link EventReceptor}
 */
public class EventReceptorTestUtils {
  protected static final int TIMEOUT_DEFAULT = 500;

  /**
   * Condition fulfilled when the {@link EventReceptor} is connected
   */
  static class EventReceptorConnected implements TestCondition {
    private final EventReceptor<?> eventReceptor;

    public EventReceptorConnected(EventReceptor<?> eventReceptor) {
      this.eventReceptor = eventReceptor;
    }

    @Override
    public boolean evaluate() {
      return eventReceptor.getStats().isConnected();
    }
  }

  /**
   * Condition fulfilled when the {@link EventReceptor} is disconnected
   */
  static class EventReceptorDisconnected implements TestCondition {
    private final EventReceptor<?> eventReceptor;

    public EventReceptorDisconnected(EventReceptor<?> eventReceptor) {
      this.eventReceptor = eventReceptor;
    }

    @Override
    public boolean evaluate() {
      return !eventReceptor.getStats().isConnected();
    }
  }

  /**
   * Waits until the specified {@link EventReceptor} is connected. It throws an
   * assertion failure otherwise
   * 
   * @param eventReceptor
   *          The event receptor
   * @param timeout
   *          The maximum number of milliseconds to wait
   * @throws InterruptedException
   *           If interrupted while waiting
   */
  public static void waitConnected(EventReceptor<?> eventReceptor, int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new EventReceptorConnected(eventReceptor), timeout)) {
      throw new AssertionError("EventReceptor not connected after " + timeout + " ms");
    }
  }

  /**
   * Waits until the specified {@link EventReceptor} is connected with a timeout
   * of {@value #TIMEOUT_DEFAULT}. It throws an assertion failure otherwise
   * 
   * @param eventReceptor
   *          The event receptor
   * @throws InterruptedException
   *           If interrupted while waiting
   */
  public static void waitConnected(EventReceptor<?> eventReceptor) throws InterruptedException {
    waitConnected(eventReceptor, TIMEOUT_DEFAULT);
  }

  /**
   * Waits until the specified {@link EventReceptor} is disconnected. It throws
   * an assertion failure otherwise
   * 
   * @param eventReceptor
   *          The event receptor
   * @param timeout
   *          The maximum number of milliseconds to wait
   * @throws InterruptedException
   *           If interrupted while waiting
   */
  public static void waitDisconnected(EventReceptor<?> eventReceptor, int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new EventReceptorDisconnected(eventReceptor), timeout)) {
      throw new AssertionError("EventReceptor not disconnected after " + timeout + " ms");
    }
  }

  /**
   * Waits until the specified {@link EventReceptor} is disconnected with a
   * timeout of {@value #TIMEOUT_DEFAULT}. It throws an assertion failure
   * otherwise
   * 
   * @param eventReceptor
   *          The event receptor
   * @throws InterruptedException
   *           If interrupted while waiting
   */
  public static void waitDisconnected(EventReceptor<?> eventReceptor) throws InterruptedException {
    waitDisconnected(eventReceptor, TIMEOUT_DEFAULT);
  }
}