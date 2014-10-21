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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation of {@link LocationService} that stores data in memory
 * for test purpose
 */
public class SimpleLocationService extends AbstractLocationService<SimpleServiceProvider> {
  private static final SimpleLocationService instance = new SimpleLocationService();
  protected static final Logger LOG = LoggerFactory.getLogger(SimpleLocationService.class);

  /**
   * @return The instance of the {@link SimpleLocationService}
   */
  public static SimpleLocationService getInstance() {
    return instance;
  }

  /**
   * Default constructor
   */
  public SimpleLocationService() {
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#start()
   */
  public boolean start() {
    LOG.info("Started");
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#stop()
   */
  public boolean stop() {
    LOG.info("Stopped");
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#register(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void register(SimpleServiceProvider serviceProvider) {
    LOG.info("Registered {}", serviceProvider);
    serviceProviderManager.add(serviceProvider);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#unregister(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void unregister(SimpleServiceProvider serviceProvider) {
    LOG.info("Unregistered {}", serviceProvider);
    serviceProviderManager.remove(serviceProvider);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#getSerialization()
   */
  public ServiceProviderSerialization<SimpleServiceProvider> getSerialization() {
    return new SimpleServiceProviderSerialization();
  }
}
