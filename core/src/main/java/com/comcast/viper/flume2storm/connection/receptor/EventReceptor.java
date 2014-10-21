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

import java.util.List;

import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * The client-side of the Flume2Storm connector, which receives the Flume2Storm
 * events from the {@link EventSender}.
 * 
 * @param <CP>
 *          The {@link ConnectionParameters} class
 */
public interface EventReceptor<CP extends ConnectionParameters> {
  /**
   * @return The connection parameters
   */
  CP getConnectionParameters();

  /**
   * @return True if the {@link EventReceptor} started successfully, false
   *         otherwise
   */
  boolean start();

  /**
   * @return True if the {@link EventReceptor} stopped successfully, false
   *         otherwise
   */
  boolean stop();

  /**
   * @return Statistics related to this {@link EventReceptor}
   */
  EventReceptorStatsMBean getStats();

  // TODO use listener instead
  /**
   * @return All the (Flume2Storm) events received
   */
  List<F2SEvent> getEvents();

  /**
   * @param maxEvents
   *          The maximum number of events to return
   * @return The (Flume2Storm) events received
   */
  List<F2SEvent> getEvents(int maxEvents);
}
