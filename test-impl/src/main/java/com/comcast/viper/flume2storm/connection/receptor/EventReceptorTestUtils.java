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

public class EventReceptorTestUtils {

  public static class EventReceptorConnected implements TestCondition {
    private final EventReceptor<?> eventReceptor;

    public EventReceptorConnected(EventReceptor<?> eventReceptor) {
      this.eventReceptor = eventReceptor;
    }

    @Override
    public boolean evaluate() {
      return eventReceptor.isConnected();
    }
  }

  public static class EventReceptorDisconnected implements TestCondition {
    private final EventReceptor<?> eventReceptor;

    public EventReceptorDisconnected(EventReceptor<?> eventReceptor) {
      this.eventReceptor = eventReceptor;
    }

    @Override
    public boolean evaluate() {
      return !eventReceptor.isConnected();
    }
  }

  public static void waitConnected(EventReceptor<?> eventReceptor, int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new EventReceptorConnected(eventReceptor), timeout)) {
      throw new AssertionError("EventReceptor not connected after " + timeout + " ms");
    }
  }

  public static void waitConnected(EventReceptor<?> eventReceptor) throws InterruptedException {
    waitConnected(eventReceptor, 500);
  }

  public static void waitDisconnected(EventReceptor<?> eventReceptor, int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new EventReceptorDisconnected(eventReceptor), timeout)) {
      throw new AssertionError("EventReceptor not disconnected after " + timeout + " ms");
    }
  }

  public static void waitDisconnected(EventReceptor<?> eventReceptor) throws InterruptedException {
    waitDisconnected(eventReceptor, 500);
  }
}