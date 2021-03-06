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
import com.comcast.viper.flume2storm.location.ServiceProvider;

/**
 * Loads a {@link ServiceProvider} from a Configuration. Implementation of this
 * factory must have a no-argument constructor.
 * 
 * @param <SP>
 *          The actual class for the ServiceProvider
 */
public interface ServiceProviderConfigurationLoader<SP extends ServiceProvider<?>> {
  /**
   * @param config
   *          The configuration to use to load a Service Provider. It contains
   *          only the subset of configuration variables to load one service
   *          provider
   * @return The newly created Service Provider
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  SP load(Configuration config) throws F2SConfigurationException;
}
