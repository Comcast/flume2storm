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

/**
 * Interface to get events about the ServiceProvider presence.
 * 
 * @param <SP>
 *          The Service Provider class
 */
public interface ServiceListener<SP extends ServiceProvider<?>> {
  /**
   * Call back method when a service provider registers to the location service
   * 
   * @param serviceProvider
   *          The service provider that was added
   */
  void onProviderAdded(SP serviceProvider);

  /**
   * Call back method when a service provider unregisters from the location
   * service
   * 
   * @param serviceProvider
   *          The service provider that disappeared
   */
  void onProviderRemoved(SP serviceProvider);
}
