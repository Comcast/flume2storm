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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.F2SEventSerializer;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptorFactory;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventFactory;
import com.comcast.viper.flume2storm.location.SimpleLocationService;
import com.comcast.viper.flume2storm.location.SimpleLocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleServiceProvider;
import com.comcast.viper.flume2storm.location.SimpleServiceProviderSerialization;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * Test {@link FlumeSpout}
 */
public class FlumeSpoutTest {
  protected static final Logger LOG = LoggerFactory.getLogger(FlumeSpoutTest.class);
  protected static final int TEST_TIMEOUT = 120000;
  private static final int BATCH_SIZE = 100;
  private static final int NB_EVENTS = 150 * BATCH_SIZE;
  private static Configuration flumeSpoutConfig;
  private static Configuration eventSender1Config;
  private static Configuration eventSender2Config;

  protected SimpleLocationService locationService;
  protected SimpleEventSender eventSender1;
  protected SimpleEventSender eventSender2;
  protected SortedSet<F2SEvent> eventsToSent;

  @BeforeClass
  public static void configure() {
    // First Event Sender configuration
    eventSender1Config = new BaseConfiguration();
    eventSender1Config.addProperty(SimpleConnectionParameters.HOSTNAME, "localhost");
    eventSender1Config.addProperty(SimpleConnectionParameters.PORT, 7001);

    // Second Event Sender configuration
    eventSender2Config = new BaseConfiguration();
    eventSender2Config.addProperty(SimpleConnectionParameters.HOSTNAME, "localhost");
    eventSender2Config.addProperty(SimpleConnectionParameters.PORT, 7002);

    // Flume Spout configuration
    flumeSpoutConfig = new BaseConfiguration();
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        SimpleLocationServiceFactory.class.getName());
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        SimpleServiceProviderSerialization.class.getName());
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS,
        SimpleEventReceptorFactory.class.getName());
  }

  @Before
  public void setup() throws F2SConfigurationException {
    // Creating and starting location service
    locationService = new SimpleLocationServiceFactory().create(null, null);
    locationService.start();

    // Creating the KryoNet servers
    eventSender1 = new SimpleEventSender(SimpleConnectionParameters.from(eventSender1Config));
    eventSender1.start();
    locationService.register(eventSender1);
    eventSender2 = new SimpleEventSender(SimpleConnectionParameters.from(eventSender2Config));
    eventSender2.start();
    locationService.register(eventSender2);

    // Generating random events
    eventsToSent = F2SEventFactory.getInstance().generateRandomEvents(NB_EVENTS * 2);
  }

  @After
  public void tearDown() throws Exception {
    // Stopping even senders
    locationService.unregister(eventSender1);
    eventSender1.stop();
    locationService.unregister(eventSender2);
    eventSender2.stop();

    // Stopping location service
    locationService.stop();
  }

  /**
   * @throws Exception
   */
  @Test
  public void testIt() throws Exception {
    final MkClusterParam mkClusterParam = new MkClusterParam();
    mkClusterParam.setSupervisors(2);
    mkClusterParam.setPortsPerSupervisor(2);
    Config daemonConf = new Config();
    daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
    mkClusterParam.setDaemonConf(daemonConf);
    Testing.withLocalCluster(new TestJob() {
      @Override
      public void run(final ILocalCluster cluster) throws Exception {
        // Building the test topology
        final TopologyBuilder builder = new TopologyBuilder();
        Set<F2SEventEmitter> eventEmitters = new HashSet<F2SEventEmitter>();
        eventEmitters.add(new BasicF2SEventEmitter());
        FlumeSpout<SimpleConnectionParameters, SimpleServiceProvider> flumeSpout = new FlumeSpout<SimpleConnectionParameters, SimpleServiceProvider>(
            eventEmitters, flumeSpoutConfig);
        builder.setSpout("FlumeSpout", flumeSpout, 2);
        final TestBolt psBolt = new TestBolt();
        builder.setBolt("TestBolt", psBolt, 2).shuffleGrouping("FlumeSpout");

        // Starting topology
        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.registerSerialization(F2SEvent.class, F2SEventSerializer.class);
        conf.setFallBackOnJavaSerialization(false);
        cluster.submitTopology("TestTopology", conf, builder.createTopology());

        Thread senderThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              // Waiting that topology is ready
              LOG.info("Waiting that receptors connect...");
              if (!TestUtils.waitFor(new TestCondition() {
                @Override
                public boolean evaluate() {
                  return eventSender1.getStats().getNbClients() == 2 && eventSender2.getStats().getNbClients() == 2;
                }
              }, TEST_TIMEOUT)) {
                Assert.fail("Receptors failed to connect to senders in time (" + TEST_TIMEOUT + " ms)");
              }
              LOG.info("Receptors connected... sending events");
              // Load balancing events between the 2 event senders
              int batchNb = 0;
              List<F2SEvent> batch = null;
              for (F2SEvent event : eventsToSent) {
                if (batch == null) {
                  batch = new ArrayList<F2SEvent>(BATCH_SIZE);
                  batchNb++;
                }
                batch.add(event);
                if (batch.size() == BATCH_SIZE) {
                  SimpleEventSender eventSender = batchNb % 2 == 0 ? eventSender1 : eventSender2;
                  eventSender.send(batch);
                  batch = null;
                }
              }
              LOG.info("Sent {} events", eventsToSent.size());
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
        senderThread.start();

        // Waiting that it's done
        if (!TestUtils.waitFor(new TestCondition() {
          @Override
          public boolean evaluate() {
            LOG.debug("Received so far: " + MemoryStorage.getInstance().getReceivedEvents().size());
            return MemoryStorage.getInstance().getReceivedEvents().size() >= NB_EVENTS * 2;
          }
        }, TEST_TIMEOUT)) {
          Assert.fail("Failed to receive all events in time (" + TEST_TIMEOUT + " ms)");
        }

        // Testing results:
        // 1 - Each sender have sent half of the events
        assertThat(eventSender1.getStats().getNbEventsIn()).isEqualTo(NB_EVENTS);
        assertThat(eventSender1.getStats().getNbEventsOut()).isEqualTo(NB_EVENTS);
        assertThat(eventSender2.getStats().getNbEventsIn()).isEqualTo(NB_EVENTS);
        assertThat(eventSender2.getStats().getNbEventsOut()).isEqualTo(NB_EVENTS);
        // TODO 2 - Each spout have received half of the events
        // 2 - We received all the events correctly
        // Programming note: I used SortedSet and iterate over the 2 sets to
        // speed up the comparison
        assertThat(MemoryStorage.getInstance().getReceivedEvents().size()).isEqualTo(eventsToSent.size());
        Iterator<F2SEvent> it1 = eventsToSent.iterator();
        Iterator<F2SEvent> it2 = MemoryStorage.getInstance().getReceivedEvents().iterator();
        while (it1.hasNext() && it2.hasNext()) {
          assertThat(it1.next()).isEqualTo(it2.next());
        }
      }
    });
  }
}
