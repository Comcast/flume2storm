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
 * A simple implementation of the {@link LocationServiceFactory} for test
 * purpose
 */
public class SimpleLocationServiceFactory implements LocationServiceFactory<SimpleServiceProvider> {
  /**
   * @see com.comcast.viper.flume2storm.location.LocationServiceFactory#create(org.apache.commons.configuration.Configuration,
   *      com.comcast.viper.flume2storm.location.ServiceProviderSerialization)
   */
  @Override
  public SimpleLocationService create(Configuration config,
      ServiceProviderSerialization<SimpleServiceProvider> serviceProviderSerialization)
      throws F2SConfigurationException {
    return SimpleLocationService.getInstance();
  }
}
