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

import java.util.List;

/**
 * Main interface for the location service. It handles registration from the
 * ServiceProvider and provides information related to their presence.
 * 
 * @param <SP>
 *          The Service Provider class
 */
public interface LocationService<SP extends ServiceProvider<?>> {
  /**
   * Starts the {@link LocationService}
   * 
   * @return True if the operation succeeded, false otherwise
   */
  boolean start();

  /**
   * Stops the {@link LocationService}
   * 
   * @return True if the operation succeeded, false otherwise
   */
  boolean stop();

  /**
   * Registers a {@link ServiceProvider} to the location service
   * 
   * @param serviceProvider
   *          The {@link ServiceProvider} that joined the service
   */
  void register(SP serviceProvider);

  /**
   * Unregisters a {@link ServiceProvider} from the location service
   * 
   * @param serviceProvider
   *          The {@link ServiceProvider} that left the service
   */
  void unregister(SP serviceProvider);

  /**
   * @return The (unmodifiable) list of information about the currently active
   *         service providers
   */
  List<SP> getServiceProviders();

  /**
   * Adds the specified listener
   * 
   * @param serviceListener
   *          A {@link ServiceListener} for the specific type of
   *          {@link ServiceProvider}
   */
  void addListener(final ServiceListener<SP> serviceListener);

  /**
   * Removes the specified listener
   * 
   * @param serviceListener
   *          A {@link ServiceListener} for the specific type of
   *          {@link ServiceProvider}
   */
  void removeListener(final ServiceListener<SP> serviceListener);

  /**
   * @return The serialization factory for this service provider
   */
  ServiceProviderSerialization<SP> getSerialization();
}
