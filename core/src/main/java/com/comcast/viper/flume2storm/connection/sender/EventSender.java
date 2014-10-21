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

import java.util.List;

import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.ServiceProvider;

/**
 * The server-side of the Flume2Storm connector, which sends the Flume2Storm
 * events to the Event Receptor.<br />
 * This is also an instance of a service provider, which is what gets registered
 * to the location service.
 * 
 * @param <CP>
 *          The Connection Parameters class
 */
public interface EventSender<CP extends ConnectionParameters> extends ServiceProvider<CP> {
  /**
   * @return True if the {@link EventSender} started successfully, false
   *         otherwise
   */
  boolean start();

  /**
   * @return True if the {@link EventSender} stopped successfully, false
   *         otherwise
   */
  boolean stop();

  /**
   * @return Statistics related to this {@link EventSender}
   */
  EventSenderStatsMBean getStats();

  /**
   * @param events
   *          The events to send
   * @return The number of events successfully sent
   */
  int send(List<F2SEvent> events);

  // TODO add listener for receptor connection/disconnection event
}
