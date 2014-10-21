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

import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.ServiceProvider;

/**
 * Statistics related to the {@link EventReceptor}
 */
// TODO check on MBean convention
public interface EventReceptorStatsMBean {
  /**
   * @return The {@link EventSender} / {@link ServiceProvider} identifier
   */
  String getEventSenderId();

  /**
   * @return True if the {@link EventReceptor} is connected
   */
  boolean isConnected();

  /**
   * @return The total number of {@link F2SEvent} ingested by the
   *         {@link EventReceptor}
   */
  long getNbEventsIn();

  /**
   * @return The number of {@link F2SEvent} currently in the the
   *         {@link EventReceptor} queue
   */
  long getNbEventsQueued();

  /**
   * Resets all the metrics
   * 
   * @return This object
   */
  EventReceptorStatsMBean reset();
}