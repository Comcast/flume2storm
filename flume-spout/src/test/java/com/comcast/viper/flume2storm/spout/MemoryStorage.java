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
package com.comcast.viper.flume2storm.spout;

import java.util.SortedSet;
import java.util.TreeSet;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventComparator;

/**
 * Stores {@link F2SEvent} in memory for test purpose
 */
public class MemoryStorage {
  private static MemoryStorage instance = new MemoryStorage();

  public static MemoryStorage getInstance() {
    return instance;
  }

  protected SortedSet<F2SEvent> receivedEvents;

  public MemoryStorage() {
    receivedEvents = new TreeSet<F2SEvent>(new F2SEventComparator());
  }

  public synchronized SortedSet<F2SEvent> getReceivedEvents() {
    return receivedEvents;
  }

  public synchronized void addEvent(F2SEvent event) {
    receivedEvents.add(event);
  }

  public synchronized void clear() {
    receivedEvents.clear();
  }
}
