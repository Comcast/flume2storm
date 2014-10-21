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

import java.util.HashMap;
import java.util.Map;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptor;

/**
 * This singleton class allows the {@link SimpleEventReceptor} to connect to the
 * {@link SimpleEventSender}
 */
public class SimpleEventSenderRouter {
  private static final SimpleEventSenderRouter instance = new SimpleEventSenderRouter();
  protected final Map<SimpleConnectionParameters, SimpleEventSender> senders;

  /**
   * @return The {@link SimpleEventSenderRouter} instance
   */
  public static SimpleEventSenderRouter getInstance() {
    return instance;
  }

  private SimpleEventSenderRouter() {
    senders = new HashMap<SimpleConnectionParameters, SimpleEventSender>();
  }

  /**
   * Registers the {@link SimpleEventSender}
   * 
   * @param sender
   *          A {@link SimpleEventSender}
   */
  public void add(SimpleEventSender sender) {
    senders.put(sender.getConnectionParameters(), sender);
  }

  /**
   * Unregisters the {@link SimpleEventSender}
   * 
   * @param sender
   *          A {@link SimpleEventSender}
   */
  public void remove(SimpleEventSender sender) {
    senders.remove(sender.getConnectionParameters());
  }

  /**
   * @param connectionParameters
   *          Some connection parameters
   * @return The {@link SimpleEventSender} associated with the specified
   *         connection parameters
   */
  public SimpleEventSender get(SimpleConnectionParameters connectionParameters) {
    return senders.get(connectionParameters);
  }
}