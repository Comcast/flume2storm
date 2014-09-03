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

import java.util.ArrayList;
import java.util.List;

import com.comcast.viper.flume2storm.location.ServiceListener;

/**
 * A simple implementation of ServiceListener that stores in memory the last
 * SimpleServiceProviders that were added or removed (for test/example purpose)
 */
public class SimpleServiceListener implements ServiceListener<SimpleServiceProvider> {
  protected final List<SimpleServiceProvider> lastAdded;
  protected final List<SimpleServiceProvider> lastRemoved;

  /**
   * Default constructor
   */
  public SimpleServiceListener() {
    lastAdded = new ArrayList<SimpleServiceProvider>();
    lastRemoved = new ArrayList<SimpleServiceProvider>();
  }

  /**
   * Clears the list of recently added and removed service providers
   */
  public void clear() {
    lastAdded.clear();
    lastRemoved.clear();
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderAdded(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void onProviderAdded(SimpleServiceProvider serviceProvider) {
    lastAdded.add(serviceProvider);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderRemoved(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void onProviderRemoved(SimpleServiceProvider serviceProvider) {
    lastRemoved.add(serviceProvider);
  }

  /**
   * @return The list of service provider added
   */
  public List<SimpleServiceProvider> getLastAdded() {
    return lastAdded;
  }

  /**
   * @return The list of service provider removed
   */
  public List<SimpleServiceProvider> getLastRemoved() {
    return lastRemoved;
  }
}
