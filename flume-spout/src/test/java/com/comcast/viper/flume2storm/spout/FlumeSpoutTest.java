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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;

import com.comcast.viper.flume2storm.F2SEventSerializer;
import com.comcast.viper.flume2storm.KryoNetParameters;
import com.comcast.viper.flume2storm.connection.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventComparator;
import com.comcast.viper.flume2storm.event.F2SEventFactory;
import com.comcast.viper.flume2storm.location.DynamicLocationService;
import com.comcast.viper.flume2storm.location.DynamicLocationServiceConfiguration;
import com.comcast.viper.flume2storm.location.DynamicLocationServiceFactory;
import com.comcast.viper.flume2storm.location.KryoNetServiceProvider;
import com.comcast.viper.flume2storm.location.KryoNetServiceProviderSerialization;
import com.comcast.viper.flume2storm.sender.KryoNetEventSender;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkServerTestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkTestUtils;

public class FlumeSpoutTest {
  protected static final int TEST_TIMEOUT = 20000;
  private static final int BATCH_SIZE = 100;
  private static final int NB_EVENTS = 150 * BATCH_SIZE;
  private static MapConfiguration kryoConfig;
  private static MapConfiguration locationServiceConfig;
  private static CombinedConfiguration flumeSpoutConfig;
  private static Configuration eventSender1Config;
  private static Configuration eventSender2Config;

  private KryoNetEventSender eventSender1;
  private KryoNetEventSender eventSender2;
  private DynamicLocationService<KryoNetServiceProvider> locationService;
  private SortedSet<F2SEvent> eventsToSent;

  private static SortedSet<F2SEvent> generateRandomEvents(int nbEvents) {
    final SortedSet<F2SEvent> result = new TreeSet<F2SEvent>(new F2SEventComparator());
    while (result.size() != nbEvents) {
      result.add(F2SEventFactory.getInstance().createRandomWithHeaders());
    }
    return result;
  }

  @BeforeClass
  public static void configure() {
    // Global KryoNet configuration
    kryoConfig = new MapConfiguration(new HashMap<String, Object>());
    kryoConfig.addProperty(KryoNetParameters.CONNECTION_TIMEOUT, 500);
    kryoConfig.addProperty(KryoNetParameters.RECONNECTION_DELAY, 1000);
    kryoConfig.addProperty(KryoNetParameters.TERMINATION_TO, 2000);

    // Location Service configuration
    locationServiceConfig = new MapConfiguration(new HashMap<String, Object>());
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.CONNECTION_STRING, ZkTestUtils.HOST + ":"
        + ZkTestUtils.PORT);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.SESSION_TIMEOUT, 2000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.CONNECTION_TIMEOUT, 500);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.RECONNECTION_DELAY, 1000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.TERMINATION_TIMEOUT, 2000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.BASE_PATH, "/unitTest");
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.SERVICE_NAME, "ut");

    // First Event Sender configuration
    eventSender1Config = new MapConfiguration(new HashMap<String, Object>());
    eventSender1Config.addProperty(KryoNetConnectionParameters.HOSTNAME, "localhost");
    eventSender1Config.addProperty(KryoNetConnectionParameters.PORT, 7001);

    // Second Event Sender configuration
    eventSender2Config = new MapConfiguration(new HashMap<String, Object>());
    eventSender2Config.addProperty(KryoNetConnectionParameters.HOSTNAME, "localhost");
    eventSender2Config.addProperty(KryoNetConnectionParameters.PORT, 7002);

    // Flume Spout configuration
    flumeSpoutConfig = new CombinedConfiguration();
    flumeSpoutConfig.addConfiguration(kryoConfig, "Kryo Configuration", KryoNetParameters.CONFIG_BASE_NAME);
    flumeSpoutConfig.addConfiguration(locationServiceConfig, "Location Service Configuration",
        DynamicLocationServiceFactory.CONFIG_BASE_NAME);
    ConfigurationUtils.dump(flumeSpoutConfig, System.out);
  }

  @Before
  public void setup() throws Exception {
    // Creating Zk Cluster
    // TODO: Need a way to change the JMX port, as Storm will kick off
    // another instance which conflicts with this one
    ZkServerTestUtils.startZkServer();
    ZkServerTestUtils.waitZkServerOn();

    // Creating Dynamic location service
    DynamicLocationServiceConfiguration config = DynamicLocationServiceConfiguration.from(locationServiceConfig);
    locationService = new DynamicLocationService<KryoNetServiceProvider>(config,
        new KryoNetServiceProviderSerialization());
    locationService.start();
    TestUtils.waitFor(new TestCondition() {
      @Override
      public boolean evaluate() {
        return locationService.isConnected();
      }
    }, TEST_TIMEOUT);

    // Creating the KryoNet servers
    KryoNetParameters knParams = KryoNetParameters.from(kryoConfig);
    eventSender1 = new KryoNetEventSender(KryoNetConnectionParameters.from(eventSender1Config), knParams);
    eventSender1.start();
    locationService.register(eventSender1);
    eventSender2 = new KryoNetEventSender(KryoNetConnectionParameters.from(eventSender2Config), knParams);
    eventSender2.start();
    locationService.register(eventSender2);

    // Generating random events
    eventsToSent = generateRandomEvents(NB_EVENTS * 2);
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

    // Stopping ZkServer
    ZkServerTestUtils.stopZkServer();
    System.out.println("Server stopped.");
    ZkServerTestUtils.waitZkServerOff();
    System.out.println("All done.");
  }

  /**
   * @throws Exception
   */
  @Test
  public void testIt() throws Exception {
    final MkClusterParam mkClusterParam = new MkClusterParam();
    mkClusterParam.setSupervisors(1);
    mkClusterParam.setPortsPerSupervisor(4);
    Testing.withSimulatedTimeLocalCluster(new TestJob() {
      @Override
      public void run(final ILocalCluster cluster) throws Exception {
        // Building the test topology
        final TopologyBuilder builder = new TopologyBuilder();
        Set<F2SEventEmitter> eventEmitters = new HashSet<F2SEventEmitter>();
        eventEmitters.add(new BasicF2SEventEmitter());
        FlumeSpout<KryoNetConnectionParameters, KryoNetServiceProvider> flumeSpout = new FlumeSpout<KryoNetConnectionParameters, KryoNetServiceProvider>(
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
              if (!TestUtils.waitFor(new TestCondition() {
                @Override
                public boolean evaluate() {
                  return eventSender1.getStats().getNbClients() > 0 && eventSender2.getStats().getNbClients() > 0;
                }
              }, TEST_TIMEOUT, 50)) {
                Assert.fail("Receptors failed to connect to senders in time (" + TEST_TIMEOUT + " ms)");
              }
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
                  KryoNetEventSender eventSender = batchNb % 2 == 0 ? eventSender1 : eventSender2;
                  System.out.println("Sending batch!");
                  eventSender.send(batch);
                  batch = null;
                }
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
        senderThread.start();

        // Waiting that it's done
        final long t0 = System.currentTimeMillis();
        while (MemoryStorage.getInstance().getReceivedEvents().size() < NB_EVENTS * 2
            && (System.currentTimeMillis() - t0) < TEST_TIMEOUT) {
          Testing.advanceClusterTime(cluster, 1);
          System.out.println("Received so far: " + MemoryStorage.getInstance().getReceivedEvents().size());
          Thread.sleep(100);
        }
        System.out.println("almost done....");
        senderThread.join(TEST_TIMEOUT);
        System.out.println("senderthread terminated....");

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
