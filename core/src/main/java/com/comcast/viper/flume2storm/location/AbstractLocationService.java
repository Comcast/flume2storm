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
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * Implements the methods to deal with ServiceListener.
 * 
 * @param <SP>
 *          The Service Provider class
 */
public abstract class AbstractLocationService<SP extends ServiceProvider<?>> implements LocationService<SP> {
  /*
   * Access to the methods to add and remove listeners is synchronized so that
   * building the immutable list of service providers is thread-safe. This is a
   * small price to pay (as it should not occur too often) so that future access
   * to the list is direct atomic reference to the immutable list.
   */
  protected final AtomicReference<List<ServiceListener<SP>>> listeners;
  protected final ServiceListener<SP> managerListener;
  protected final ServiceProviderManager<SP> serviceProviderManager;

  /**
   * This {@link ServiceListener} gets the notifications from the
   * {@link ServiceProviderManager} and dispatches it to the registered
   * listeners
   */
  private class MainServiceListener implements ServiceListener<SP> {
    private final Logger LOG = LoggerFactory.getLogger(MainServiceListener.class);

    protected MainServiceListener() {
      super(); // To avoid synthetic accessor method
    }

    /**
     * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderAdded(com.comcast.viper.flume2storm.location.ServiceProvider)
     */
    public void onProviderAdded(SP serviceProvider) {
      for (final ServiceListener<SP> l : listeners.get()) {
        try {
          l.onProviderAdded(serviceProvider);
        } catch (Exception e) {
          LOG.warn("Failed to notify listeners about the addition of service provider {} : {}", serviceProvider,
              e.getLocalizedMessage());
        }
      }
    }

    /**
     * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderRemoved(com.comcast.viper.flume2storm.location.ServiceProvider)
     */
    public void onProviderRemoved(SP serviceProvider) {
      for (final ServiceListener<SP> l : listeners.get()) {
        try {
          l.onProviderRemoved(serviceProvider);
        } catch (Exception e) {
          LOG.warn("Failed to notify listeners about the removal of service provider {} : {}", serviceProvider,
              e.getLocalizedMessage());
        }
      }
    }
  }

  protected AbstractLocationService() {
    listeners = new AtomicReference<List<ServiceListener<SP>>>(new ArrayList<ServiceListener<SP>>());
    managerListener = new MainServiceListener();
    serviceProviderManager = new ServiceProviderManager<SP>(managerListener);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#addListener(com.comcast.viper.flume2storm.location.ServiceListener)
   */
  public synchronized final void addListener(ServiceListener<SP> listener) {
    List<ServiceListener<SP>> oldList = listeners.get();
    Builder<ServiceListener<SP>> builder = new Builder<ServiceListener<SP>>();
    builder.addAll(oldList);
    builder.add(listener);
    listeners.getAndSet(builder.build());
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#removeListener(com.comcast.viper.flume2storm.location.ServiceListener)
   */
  public synchronized final void removeListener(ServiceListener<SP> listener) {
    List<ServiceListener<SP>> oldList = new ArrayList<ServiceListener<SP>>(listeners.get());
    oldList.remove(listener);
    listeners.getAndSet(ImmutableList.copyOf(oldList));
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#getServiceProviders()
   */
  @Override
  public final List<SP> getServiceProviders() {
    return serviceProviderManager.get();
  }

}
