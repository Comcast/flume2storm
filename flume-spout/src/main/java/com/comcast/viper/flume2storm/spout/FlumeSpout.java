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
package com.comcast.viper.flume2storm.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptor;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptorFactory;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.LocationService;
import com.comcast.viper.flume2storm.location.LocationServiceFactory;
import com.comcast.viper.flume2storm.location.ServiceListener;
import com.comcast.viper.flume2storm.location.ServiceProvider;
import com.comcast.viper.flume2storm.location.ServiceProviderSerialization;

/**
 * A Storm spout that ingests data from Flume, via the Flume2Storm connector.
 * This is one of the 2 main components of the Flume2Storm connector. On
 * reception of Flume2Storm events, it queues them in-memory, and emit them on
 * {@link #nextTuple()} call.
 * <p />
 * In order to connect it to the rest of the Storm topology, use one or more
 * {@link F2SEventEmitter}.
 * 
 * @param <CP>
 *          The Connection Parameters class
 * @param <SP>
 *          The Service Provider class
 */
public class FlumeSpout<CP extends ConnectionParameters, SP extends ServiceProvider<CP>> extends BaseRichSpout {
  private static final long serialVersionUID = -2858136141243917853L;
  /** Small sleep for storm spout */
  protected static final int SPOUT_SLEEP_TIME = 10;
  protected static final Logger LOG = LoggerFactory.getLogger(FlumeSpout.class);

  protected final FlumeSpoutConfiguration configuration;
  protected final Map<String, EventReceptor<CP>> eventReceptors;
  protected final Set<F2SEventEmitter> emitters;
  protected SpoutOutputCollector collector;
  protected LocationServiceFactory<SP> locationServiceFactory;
  protected ServiceProviderSerialization<SP> serviceProviderSerialization;
  protected LocationService<SP> locationService;
  protected EventReceptorFactory<CP> eventReceptorFactory;
  protected ServiceListener<SP> serviceListener;

  private class MyServiceListener implements ServiceListener<SP> {
    protected MyServiceListener() {
      super();
    }

    /**
     * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderRemoved(com.comcast.viper.flume2storm.location.ServiceProvider)
     */
    @Override
    public void onProviderRemoved(SP serviceProvider) {
      LOG.info("Removed provider: {}", serviceProvider);
    }

    /**
     * @see com.comcast.viper.flume2storm.location.ServiceListener#onProviderAdded(com.comcast.viper.flume2storm.location.ServiceProvider)
     */
    @Override
    public void onProviderAdded(SP serviceProvider) {
      try {
        LOG.info("Added provider: {}", serviceProvider);
        EventReceptor<CP> eventReceptor = eventReceptorFactory.create(serviceProvider.getConnectionParameters(),
            configuration.get());
        LOG.debug("Adding event receptor: {}", eventReceptor);
        eventReceptor.start();
        eventReceptors.put(serviceProvider.getId(), eventReceptor);
      } catch (F2SConfigurationException e) {
        LOG.error("Failed to add service provider: " + serviceProvider, e);
      }
    }
  }

  /**
   * Constructs a new {@link FlumeSpout}
   * 
   * @param emitters
   *          The list of {@link F2SEventEmitter}
   * @param configuration
   *          The configuration for the spout
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public FlumeSpout(final Set<F2SEventEmitter> emitters, final Configuration configuration)
      throws F2SConfigurationException {
    this.emitters = emitters;
    eventReceptors = new HashMap<String, EventReceptor<CP>>();
    this.configuration = FlumeSpoutConfiguration.from(configuration);
  }

  /**
   * @see backtype.storm.spout.ISpout#open(java.util.Map,
   *      backtype.storm.task.TopologyContext,
   *      backtype.storm.spout.SpoutOutputCollector)
   */
  @SuppressWarnings("unchecked")
  @Override
  public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
      final SpoutOutputCollector collector) {
    try {
      LOG.debug("Opening...");
      this.collector = collector;
      Class<? extends LocationServiceFactory<SP>> locationServiceFactoryClass = (Class<? extends LocationServiceFactory<SP>>) Class
          .forName(configuration.getLocationServiceFactoryClassName());
      this.locationServiceFactory = locationServiceFactoryClass.newInstance();
      Class<? extends ServiceProviderSerialization<SP>> serviceProviderSerializationClass = (Class<? extends ServiceProviderSerialization<SP>>) Class
          .forName(configuration.getServiceProviderSerializationClassName());
      this.serviceProviderSerialization = serviceProviderSerializationClass.newInstance();
      Class<? extends EventReceptorFactory<CP>> eventReceptorFactoryClass = (Class<? extends EventReceptorFactory<CP>>) Class
          .forName(configuration.getEventReceptorFactoryClassName());
      this.eventReceptorFactory = eventReceptorFactoryClass.newInstance();
      locationService = locationServiceFactory.create(configuration.get(), serviceProviderSerialization);
      serviceListener = new MyServiceListener();
      locationService.addListener(serviceListener);
      locationService.start();
      LOG.info("Opened");
    } catch (Exception e) {
      LOG.error("Failed to open properly: " + e.getMessage(), e);
    }
  }

  /**
   * @see backtype.storm.topology.base.BaseRichSpout#close()
   */
  @Override
  public void close() {
    LOG.debug("Closing...");
    locationService.removeListener(serviceListener);
    locationService.stop();
    for (EventReceptor<CP> eventReceptor : eventReceptors.values()) {
      eventReceptor.stop();
    }
    eventReceptors.clear();
    LOG.info("Clossed");
  }

  // This is probably not needed, because we don't remove the service provider
  // if the connection to ZK fails:
  @Deprecated
  protected void removeServiceProvider(SP serviceProvider) {
    EventReceptor<CP> eventReceptor = eventReceptors.remove(serviceProvider.getId());
    if (eventReceptor != null) {
      eventReceptor.stop();
      LOG.debug("Removed service provider: {}", eventReceptor);
    }
  }

  /**
   * @see backtype.storm.spout.ISpout#nextTuple()
   */
  @Override
  public void nextTuple() {
    try {
      if (collector != null) {
        // Emit any events we have queued
        for (final EventReceptor<CP> eventReceptor : eventReceptors.values()) {
          for (final F2SEvent event : eventReceptor.getEvents()) {
            LOG.trace("Received F2S event: {}", event);
            for (final F2SEventEmitter emitter : emitters) {
              emitter.emitEvent(event, collector);
            }
          }
          if (!eventReceptor.isConnected()) {
            if (!locationService.getServiceProviders().contains(eventReceptor)) {
              LOG.debug("Removing service provider: {}", eventReceptor);
              eventReceptors.remove(eventReceptor);
            }
          }
        }
      }
    } catch (final Exception ex) {
      LOG.error("There was a problem emitting events: " + ex.getLocalizedMessage(), ex);
      collector.reportError(ex);
    }
    // Always sleep for a little bit
    try {
      Thread.sleep(SPOUT_SLEEP_TIME);
    } catch (final InterruptedException ex) {
      LOG.warn("Thread interupted: " + ex.getLocalizedMessage());
    }
  }

  /**
   * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
   */
  @Override
  public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    for (final F2SEventEmitter emitter : emitters) {
      emitter.declareOutputFields(declarer);
    }
  }
}
