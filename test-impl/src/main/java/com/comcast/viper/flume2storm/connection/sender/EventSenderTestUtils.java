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

public class EventSenderTestUtils {

  public static class NoReceptorCondition implements TestCondition {
    private final EventSender<?> sender;

    public NoReceptorCondition(final EventSender<?> sender) {
      this.sender = sender;
    }

    @Override
    public boolean evaluate() {
      return sender.getNbReceptors() == 0;
    }
  }

  /**
   * Waits that there is no receptor connected to the sender for a max of
   * timeout. It throws an assertion failure otherwise
   * 
   * @param sender
   *          The {@link KryoNetEventSender}
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
   * Waits that there is no receptor connected to the sender for a default max
   * of timeout. It throws an assertion failure otherwise
   * 
   * @param sender
   *          The {@link KryoNetEventSender}
   * @throws InterruptedException
   */
  public static void waitNoReceptor(EventSender<?> sender) throws InterruptedException {
    waitNoReceptor(sender, 500);
  }
}