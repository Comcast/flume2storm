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
 * Interface to build a Location Service. It follows the abstract factory design
 * pattern. Implementation of this factory must have a no-argument constructor.
 * 
 * @param <SP>
 *          The Service Provider class
 */
public interface LocationServiceFactory<SP extends ServiceProvider<?>> {
  /**
   * Creates a new {@link LocationService} based on the configuration provided
   * 
   * @param config
   *          Configuration for the location service
   * @param serviceProviderSerialization
   *          The {@link ServiceProviderSerialization} to use
   * @return The newly created {@link LocationService}
   * @throws F2SConfigurationException
   *           If the configuration specified is invalid
   */
  LocationService<SP> create(Configuration config, ServiceProviderSerialization<SP> serviceProviderSerialization)
      throws F2SConfigurationException;
}
