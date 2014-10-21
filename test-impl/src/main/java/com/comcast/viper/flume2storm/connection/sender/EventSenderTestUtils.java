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
package com.comcast.viper.flume2storm.connection.sender;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * Utilities to facilitate testing of an {@link EventSender}
 */
public class EventSenderTestUtils {
  protected static final int TIMEOUT_DEFAULT = 500;

  /**
   * Condition fulfilled when the {@link EventSender} does not have any
   */
  static class NoReceptorCondition implements TestCondition {
    private final EventSender<?> sender;

    public NoReceptorCondition(final EventSender<?> sender) {
      this.sender = sender;
    }

    @Override
    public boolean evaluate() {
      return sender.getStats().getNbClients() == 0;
    }
  }

  /**
   * Waits that there is no receptor connected to the sender. It throws an
   * assertion failure otherwise
   * 
   * @param sender
   *          The {@link EventSender}
   * @param timeout
   *          Max wait time, in milliseconds
   * @throws InterruptedException
   */
  public static void waitNoReceptor(EventSender<?> sender, final int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new NoReceptorCondition(sender), timeout)) {
      throw new AssertionError("There were still some receptor(s) connected  (after " + timeout + " ms)");
    }
  }

  /**
   * Waits that there is no receptor connected to the sender with a timeout of
   * {@value #TIMEOUT_DEFAULT} . It throws an assertion failure otherwise
   * 
   * @param sender
   *          The {@link EventSender}
   * @throws InterruptedException
   */
  public static void waitNoReceptor(EventSender<?> sender) throws InterruptedException {
    waitNoReceptor(sender, TIMEOUT_DEFAULT);
  }
}