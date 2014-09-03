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
package com.comcast.viper.flume2storm.location;

import java.io.Serializable;

import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;

/**
 * Represents an instance of a service provider, which is what gets registered
 * to the location service.
 * 
 * @param <CP>
 *          The Connection Parameters class
 */
public interface ServiceProvider<CP extends ConnectionParameters> extends Serializable, Comparable<ServiceProvider<CP>> {
  /**
   * @return A unique identifier for the Service provider (hostname:port for
   *         instance)
   */
  String getId();

  /**
   * @return The connection parameters for this service provider
   */
  CP getConnectionParameters();
}
