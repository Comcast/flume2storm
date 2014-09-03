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

import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * Listener for EventReceptor events.
 */
public interface EventReceptorListener {
  /**
   * Callback method on {@link EventReceptor} connection event
   */
  void onConnection();

  /**
   * Callback method on {@link EventReceptor} disconnection event
   */
  void onDisconnection();

  /**
   * Callback method on Flume2Storm events reception
   * 
   * @param events
   *          The Flume2Storm events received
   */
  void onEvent(List<F2SEvent> events);
}
