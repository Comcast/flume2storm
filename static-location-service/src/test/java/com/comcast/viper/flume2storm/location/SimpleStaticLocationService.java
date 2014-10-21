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

import org.apache.commons.configuration.Configuration;

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * A simple implementation of LocationService that stores data in memory for
 * test purpose.
 */
public class SimpleStaticLocationService extends StaticLocationService<SimpleServiceProvider> {
  /**
   * @param config
   *          A configuration containing both the StaticLocationService
   *          configuration and the service provider declarations
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public SimpleStaticLocationService(Configuration config) throws F2SConfigurationException {
    super(config, new SimpleServiceProviderSerialization());
  }
}
