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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptor;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptorTestUtils;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptor;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSender;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSenderFactory;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.LocationService;
import com.comcast.viper.flume2storm.location.SimpleLocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleServiceProvider;
import com.comcast.viper.flume2storm.location.SimpleServiceProviderSerialization;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.google.common.collect.ImmutableMap;

import static org.assertj.core.api.Assertions.assertThat;

public class StormSinkTest {
  private static MapConfiguration stormSinkConfig;
  private static LocationService<SimpleServiceProvider> simpleLocationService;

  @BeforeClass
  public static void configure() {
    // Storm sink configuration
    stormSinkConfig = new MapConfiguration(new HashMap<String, Object>());
    stormSinkConfig.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        SimpleLocationServiceFactory.class.getName());
    stormSinkConfig.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        SimpleServiceProviderSerialization.class.getName());
    stormSinkConfig.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS,
        SimpleEventSenderFactory.class.getName());
    stormSinkConfig.addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS,
        SimpleConnectionParametersFactory.class.getName());
  }

  @Before
  public void setup() throws F2SConfigurationException {
    // Creating the location service
    SimpleLocationServiceFactory simpleLocationServiceFactory = new SimpleLocationServiceFactory();
    simpleLocationService = simpleLocationServiceFactory.create(null, null);
    simpleLocationService.start();

  }

  @After
  public void tearDown() throws Exception {
    // Stopping location service
    simpleLocationService.stop();
  }

  /**
   */
  @Test
  public void testIt() throws InterruptedException, LifecycleException, EventDeliveryException {
    // Starting Flume
    StormSink<SimpleConnectionParameters, SimpleEventSender> sink = new StormSink<SimpleConnectionParameters, SimpleEventSender>();

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context(stormSinkConfig.getMap());
    context.put("keep-alive", "1"); // Speeds up test
    Configurables.configure(channel, context);
    Configurables.configure(sink, context);

    sink.setChannel(channel);
    sink.start();

    // Checking location service registration
    List<SimpleServiceProvider> serviceProviders = simpleLocationService.getServiceProviders();
    assertThat(serviceProviders).hasSize(1);
    SimpleConnectionParameters connectionParams = serviceProviders.get(0).getConnectionParameters();

    // Queuing up some messages
    int nbEvents = 2 * 20; // Make it even and lower than channel capacity
    for (int i = 0; i < nbEvents; i++) {
      channel.put(EventBuilder.withBody(("Test queued" + i).getBytes()));
    }
    sink.process();

    SinkRunner sinkRunner = new SinkRunner();

    // Adding a couple of receptors
    SimpleEventReceptor eventReceptor1 = new SimpleEventReceptor(connectionParams);
    eventReceptor1.start();
    SimpleEventReceptor eventReceptor2 = new SimpleEventReceptor(connectionParams);
    eventReceptor2.start();
    EventReceptorTestUtils.waitConnected(eventReceptor1);
    EventReceptorTestUtils.waitConnected(eventReceptor2);

    // Sending queued events
    sink.process();
    assertThat(eventReceptor1.getEvents()).hasSize(nbEvents / 2);
    assertThat(eventReceptor2.getEvents()).hasSize(nbEvents / 2);

    // No event in channel
    sink.process();

    // Sending in real-time
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < nbEvents; i++) {
        channel.put(EventBuilder.withBody(("Test realtime" + i).getBytes()));
      }
      sink.process();
      assertThat(eventReceptor1.getEvents()).hasSize(nbEvents / 2);
      assertThat(eventReceptor2.getEvents()).hasSize(nbEvents / 2);
    }

    // Terminating test
    eventReceptor1.stop();
    eventReceptor2.stop();
    EventReceptorTestUtils.waitDisconnected(eventReceptor1);
    EventReceptorTestUtils.waitDisconnected(eventReceptor2);

    sink.stop();
  }
}