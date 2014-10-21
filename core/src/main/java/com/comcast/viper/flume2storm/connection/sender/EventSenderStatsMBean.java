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

import com.comcast.viper.flume2storm.connection.receptor.EventReceptor;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.ServiceProvider;

/**
 * Statistics related to the {@link EventSender}
 */
public interface EventSenderStatsMBean {

  /**
   * @return The {@link EventSender} / {@link ServiceProvider} identifier
   */
  String getEventSenderId();

  /**
   * @return The number of {@link EventReceptor} connected
   */
  int getNbClients();

  /**
   * @return The total number of {@link F2SEvent} ingested by the
   *         {@link EventSender}
   */
  long getNbEventsIn();

  /**
   * @return The total number of {@link F2SEvent} sent by the
   *         {@link EventSender}
   */
  long getNbEventsOut();

  /**
   * @return The total number of {@link F2SEvent} that failed to be sent by the
   *         {@link EventSender}
   */
  long getNbEventsFailed();

  /**
   * Resets all the metrics
   * 
   * @return This object
   */
  EventSenderStatsMBean reset();
}