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
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;

/**
 * Implementation of the {@link EventSenderFactory} for test purpose
 */
public class SimpleEventSenderFactory implements EventSenderFactory<SimpleConnectionParameters> {
  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderFactory#create(com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters,
   *      org.apache.commons.configuration.Configuration)
   */
  @Override
  public EventSender<SimpleConnectionParameters> create(SimpleConnectionParameters connectionParams,
      Configuration config) throws F2SConfigurationException {
    return new SimpleEventSender(connectionParams);
  }
}
