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
package com.comcast.viper.flume2storm.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.connection.sender.EventSenderFactory;
import com.comcast.viper.flume2storm.connection.sender.EventSenderStats;
import com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.LocationService;
import com.comcast.viper.flume2storm.location.LocationServiceFactory;
import com.comcast.viper.flume2storm.location.ServiceProvider;
import com.comcast.viper.flume2storm.location.ServiceProviderSerialization;
import com.google.common.base.Preconditions;

/**
 * This Flume sink sends the Flume events picked up from the channel to the
 * clients connected to it, load-balancing the data between them.
 * 
 * @param <CP>
 *          The Connection Parameters class
 * @param <SP>
 *          The Service Provider class
 */
public class StormSink<CP extends ConnectionParameters, SP extends ServiceProvider<CP>> extends AbstractSink implements
    Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(StormSink.class);
  protected SinkCounter sinkCounter;
  protected StormSinkConfiguration configuration;
  protected LocationServiceFactory<SP> locationServiceFactory;
  protected LocationService<SP> locationService;
  protected ServiceProviderSerialization<SP> serviceProviderSerialization;
  protected EventSenderFactory<CP> eventSenderFactory;
  protected ConnectionParametersFactory<CP> connectionParametersFactory;
  protected EventSender<CP> eventSender;
  protected EventConverter eventConverter;

  /**
   * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
   */
  @Override
  public void configure(final Context context) {
    Preconditions.checkNotNull(context);
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
    try {
      Configuration mapConfiguration = new MapConfiguration(context.getParameters());
      LOG.debug("Storm-sink configuration:\n{}", ConfigurationUtils.toString(mapConfiguration));
      configuration = StormSinkConfiguration.from(mapConfiguration);
      Class<? extends ConnectionParametersFactory<CP>> connectionParametersFactoryClass = (Class<? extends ConnectionParametersFactory<CP>>) Class
          .forName(configuration.getConnectionParametersFactoryClassName());
      this.connectionParametersFactory = connectionParametersFactoryClass.newInstance();
      Class<? extends EventSenderFactory<CP>> eventSenderFactoryClass = (Class<? extends EventSenderFactory<CP>>) Class
          .forName(configuration.getEventSenderFactoryClassName());
      this.eventSenderFactory = eventSenderFactoryClass.newInstance();
      Class<? extends LocationServiceFactory<SP>> locationServiceFactoryClass = (Class<? extends LocationServiceFactory<SP>>) Class
          .forName(configuration.getLocationServiceFactoryClassName());
      this.locationServiceFactory = locationServiceFactoryClass.newInstance();
      Class<? extends ServiceProviderSerialization<SP>> serviceProviderSerializationClass = (Class<? extends ServiceProviderSerialization<SP>>) Class
          .forName(configuration.getServiceProviderSerializationClassName());
      this.serviceProviderSerialization = serviceProviderSerializationClass.newInstance();
    } catch (Exception e) {
      LOG.error("Failed to configure Storm sink: " + e.getMessage(), e);
    }
  }

  /**
   * @see org.apache.flume.sink.AbstractSink#start()
   */
  @Override
  public synchronized void start() {
    try {
      LOG.debug("Opening...");
      sinkCounter.start();
      CP connectionParameters = connectionParametersFactory.create(configuration.get());
      eventSender = eventSenderFactory.create(connectionParameters, configuration.get());
      eventSender.start();

      locationService = locationServiceFactory.create(configuration.get(), serviceProviderSerialization);
      locationService.start();
      locationService.register((SP) eventSender);

      eventConverter = new EventConverter();
      LOG.info("Opened");
      super.start();
    } catch (F2SConfigurationException e) {
      LOG.error(e.getLocalizedMessage(), e);
    }
  }

  /**
   * @see org.apache.flume.sink.AbstractSink#stop()
   */
  @Override
  public synchronized void stop() {
    LOG.debug("Closing...");
    locationService.unregister((SP) eventSender);
    locationService.stop();
    eventSender.stop();
    sinkCounter.stop();
    LOG.info("Closed");
    super.stop();
  }

  /**
   * @see org.apache.flume.Sink#process()
   */
  @Override
  public Status process() throws EventDeliveryException {
    if (eventSender.getStats().getNbClients() == 0) {
      // No receptor connected
      return Status.BACKOFF;
    }
    Status result = Status.READY;
    final Channel channel = getChannel();
    final Transaction transaction = channel.getTransaction();
    try {
      transaction.begin();
      List<F2SEvent> batch = new ArrayList<F2SEvent>(configuration.getBatchSize());
      for (int i = 0; i < configuration.getBatchSize(); i++) {
        final Event event = channel.take();
        if (event == null) {
          break;
        }
        batch.add(eventConverter.convert(event));
      }
      if (batch.isEmpty()) {
        sinkCounter.incrementBatchEmptyCount();
        result = Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainAttemptCount(batch.size());
        if (eventSender.send(batch) == 0) {
          throw new EventDeliveryException("No event sent to receptor");
        }
        sinkCounter.addToEventDrainSuccessCount(batch.size());
        if (batch.size() == configuration.getBatchSize()) {
          sinkCounter.incrementBatchCompleteCount();
        } else {
          sinkCounter.incrementBatchUnderflowCount();
        }
      }
      transaction.commit();
    } catch (final Throwable t) {
      transaction.rollback();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof ChannelException) {
        if (!(t.getCause() instanceof InterruptedException)) {
          LOG.error("Storm Sink " + getName() + ": Unable to get event from channel " + channel.getName(), t);
        }
        result = Status.BACKOFF;
      } else {
        throw new EventDeliveryException("Failed to accept events", t);
      }
    } finally {
      transaction.close();
    }
    return result;
  }

  /**
   * @return The stats associated with teh {@link EventSender}
   */
  public EventSenderStatsMBean getEventSernderStats() {
    return eventSender == null ? new EventSenderStats("Unknown") : eventSender.getStats();
  }

  /**
   * @return The sink counter
   */
  public SinkCounter getSinkCounter() {
    return sinkCounter;
  }
}
