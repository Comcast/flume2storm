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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Manages a list of service providers and provides join/leave notifications
 * based on a changing list of servers. This is thread-safe.
 * 
 * @param <SP>
 *          The Service Provider class
 */
class ServiceProviderManager<SP extends ServiceProvider<?>> {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceProviderManager.class);
  /** The one listener we'll notify (i.e. the location service) */
  protected final ServiceListener<SP> listener;
  /** Current list of active servers */
  protected final Map<String, SP> serviceProviders;

  public ServiceProviderManager(ServiceListener<SP> listener) {
    this.listener = listener;
    serviceProviders = new HashMap<String, SP>();
  }

  public synchronized List<SP> get() {
    return ImmutableList.copyOf(serviceProviders.values());
  }

  /**
   * Updates the list of service providers, providing add/remove notifications
   * to the listener
   * 
   * @param newList
   *          The new list of service providers
   */
  public synchronized void set(final Collection<SP> newList) {
    final List<SP> oldList = new ArrayList<SP>(serviceProviders.values());
    final Iterator<SP> it = newList.iterator();
    while (it.hasNext()) {
      final SP current = it.next();
      if (contains(current.getConnectionParameters().getId())) {
        /*
         * The element is in the current and the new list - no modification of
         * the current list, but we remove it from the old list in order to see
         * the ones we did not have already
         */
        oldList.remove(current);
      } else {
        // The element is only in the new list - adding it
        addServiceProvider(current);
      }
    }
    /*
     * At this point, all the elements of the old list that were in the old and
     * the new list have been removed, therefore, the remaining elements have
     * been removed
     */
    for (final SP current : oldList) {
      removeServiceProvider(current);
    }
  }

  protected void addServiceProvider(final SP sp) {
    SP previous = serviceProviders.put(sp.getConnectionParameters().getId(), sp);
    if (previous == null) {
      LOG.debug("Adding: {}", sp);
      try {
        listener.onProviderAdded(sp);
      } catch (final Exception e) {
        LOG.warn("Failed to notify listener about the addition of service provider {} : {}", sp,
            e.getLocalizedMessage());
      }
    }
  }

  protected void removeServiceProvider(final SP sp) {
    SP removed = serviceProviders.remove(sp.getConnectionParameters().getId());
    if (removed != null) {
      LOG.debug("Removing: {}", sp);
      try {
        listener.onProviderRemoved(sp);
      } catch (final Exception e) {
        LOG.warn("Failed to notify listener about the removal of service provider {} : {}", sp, e.getLocalizedMessage());
      }
    }
  }

  /**
   * Adds a service provider, providing a notification to the listener
   * 
   * @param sp
   *          The new service provider
   */
  public synchronized void add(final SP sp) {
    if (!contains(sp.getConnectionParameters().getId())) {
      addServiceProvider(sp);
    }
  }

  /**
   * Adds a collection of service providers, providing a notification to the
   * listener
   * 
   * @param serviceProviders
   *          The new service providers to add
   */
  public synchronized void addAll(final Collection<SP> serviceProviders) {
    for (SP sp : serviceProviders) {
      add(sp);
    }
  }

  /**
   * Removes a service provider, providing a notification to the listener
   * 
   * @param sp
   *          A service provider
   */
  public synchronized void remove(final SP sp) {
    removeServiceProvider(sp);
  }

  /**
   * Removes all the {@link ServiceProvider} from the list
   */
  public synchronized void clear() {
    for (SP sp : serviceProviders.values()) {
      removeServiceProvider(sp);
    }
  }

  /**
   * @param spId
   *          A service provider
   * @return True if the specified {@link ServiceProvider} is registered, false
   *         otherwise
   */
  public synchronized boolean contains(final String spId) {
    return serviceProviders.containsKey(spId);
  }
}
