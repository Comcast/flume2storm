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

import org.apache.commons.configuration.Configuration;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;

/**
 * Interface to build an Event Sender. It follows the abstract factory design
 * pattern. Implementation of this factory must have a no-argument constructor.
 * 
 * @param <CP>
 *          The Connection Parameters class
 */
public interface EventSenderFactory<CP extends ConnectionParameters> {
  /**
   * Creates a new {@link EventSender} based on the connection parameters
   * provided
   * 
   * @param connectionParams
   *          Connections parameters to use to configure the {@link EventSender}
   * @param config
   *          Additional configuration for the creation of the event sender
   * @return The newly created {@link EventSender}
   * @throws F2SConfigurationException
   *           If the configuration specified is invalid
   */
  EventSender<CP> create(CP connectionParams, Configuration config) throws F2SConfigurationException;
}
