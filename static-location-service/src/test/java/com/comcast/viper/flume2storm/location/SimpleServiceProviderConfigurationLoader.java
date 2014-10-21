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
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;

/**
 * Loads Simple Service Provider from an Apache Configuration
 */
public class SimpleServiceProviderConfigurationLoader implements
    ServiceProviderConfigurationLoader<SimpleServiceProvider> {
  /**
   * @throws F2SConfigurationException
   * @see com.comcast.viper.flume2storm.location.ServiceProviderConfigurationLoader#load(org.apache.commons.configuration.Configuration)
   */
  public SimpleServiceProvider load(Configuration config) throws F2SConfigurationException {
    return new SimpleServiceProvider(SimpleConnectionParameters.from(config));
  }
}
