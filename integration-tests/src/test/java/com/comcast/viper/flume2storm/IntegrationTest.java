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
package com.comcast.viper.flume2storm;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkRunner;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.LoadBalancingSinkProcessor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.KillOptions;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;

import com.comcast.viper.flume2storm.connection.KryoNetParameters;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.receptor.KryoNetEventReceptorFactory;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptorFactory;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.connection.sender.KryoNetEventSenderFactory;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSenderFactory;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventFactory;
import com.comcast.viper.flume2storm.location.DynamicLocationServiceConfiguration;
import com.comcast.viper.flume2storm.location.DynamicLocationServiceFactory;
import com.comcast.viper.flume2storm.location.KryoNetServiceProvider;
import com.comcast.viper.flume2storm.location.KryoNetServiceProviderSerialization;
import com.comcast.viper.flume2storm.location.ServiceProvider;
import com.comcast.viper.flume2storm.location.ServiceProviderConfigurationLoader;
import com.comcast.viper.flume2storm.location.SimpleLocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleServiceProviderSerialization;
import com.comcast.viper.flume2storm.location.StaticLocationServiceConfiguration;
import com.comcast.viper.flume2storm.location.StaticLocationServiceFactory;
import com.comcast.viper.flume2storm.sink.StormSink;
import com.comcast.viper.flume2storm.sink.StormSinkConfiguration;
import com.comcast.viper.flume2storm.spout.BasicF2SEventEmitter;
import com.comcast.viper.flume2storm.spout.F2SEventEmitter;
import com.comcast.viper.flume2storm.spout.FlumeSpout;
import com.comcast.viper.flume2storm.spout.FlumeSpoutConfiguration;
import com.comcast.viper.flume2storm.spout.MemoryStorage;
import com.comcast.viper.flume2storm.spout.TestBolt;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.google.common.collect.ImmutableList;

/**
 * Integration tests that put all the components together. Notice that the only
 * difference between the use of various implementations is the configuration of
 * it all. <br />
 * Note that we're using the ZooKeeper server that is created as part of the
 * test topology when needed.
 * 
 */
public class IntegrationTest {
  private static final String TEST_TOPOLOGY_NAME = "TestTopology";
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);
  private static final String CHANNEL_CONFIG = "channel";
  private static final String SINK1_CONFIG = "sink1";
  private static final String SINK2_CONFIG = "sink2";
  private static final String SINK_PROCESSOR_CONFIG = "processor";
  private static final String SPOUT_CONFIG = "spout";
  private static final int TEST_TIMEOUT = 60000;
  private static final int HALF_NB_EVENTS = 15000;
  private static final int NB_EVENTS = HALF_NB_EVENTS * 2;
  private static final int ZK_PORT = 2000;

  /**
   * This contains all the other configuration objects (sink1, sink2, spout,
   * ...)
   */
  protected static SortedSet<F2SEvent> eventsToSent;
  protected CombinedConfiguration config;

  /**
   * Initializes integration tests
   */
  @BeforeClass
  public static void init() {
    // Generating test events
    eventsToSent = F2SEventFactory.getInstance().generateRandomEvents(NB_EVENTS);
  }

  /**
   * @throws Exception
   *           If anything went wrong
   */
  @Before
  public void setup() throws Exception {
    config = new CombinedConfiguration();
    // Flume channel configuration
    BaseConfiguration channelConfig = new BaseConfiguration();
    channelConfig.addProperty("keep-alive", "1"); // Speeds up test
    channelConfig.addProperty("capacity", "" + NB_EVENTS);
    config.addConfiguration(channelConfig, CHANNEL_CONFIG);

    MemoryStorage.getInstance().clear();
  }

  /**
   * Integration test with the test implementation of the Location Service and
   * the Connection API
   * 
   * @throws Exception
   *           If anything went wrong
   */
  // @Test
  public void testTestImpl() throws Exception {
    // Base storm sink configuration
    BaseConfiguration sinkBaseConfig = new BaseConfiguration();
    sinkBaseConfig.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        SimpleLocationServiceFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        SimpleServiceProviderSerialization.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS,
        SimpleEventSenderFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS,
        SimpleConnectionParametersFactory.class.getName());

    // First storm sink configuration
    BaseConfiguration sink1ConnectionParameters = new BaseConfiguration();
    sink1ConnectionParameters.addProperty(SimpleConnectionParameters.HOSTNAME, "host1");
    sink1ConnectionParameters.addProperty(SimpleConnectionParameters.PORT, 7001);
    CombinedConfiguration sink1Config = new CombinedConfiguration();
    sink1Config.addConfiguration(sinkBaseConfig);
    sink1Config.addConfiguration(sink1ConnectionParameters, "connectionParams",
        SimpleConnectionParametersFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink1Config, SINK1_CONFIG);

    // Second storm sink configuration
    BaseConfiguration sink2ConnectionParameters = new BaseConfiguration();
    sink2ConnectionParameters.addProperty(SimpleConnectionParameters.HOSTNAME, "host2");
    sink2ConnectionParameters.addProperty(SimpleConnectionParameters.PORT, 7002);
    CombinedConfiguration sink2Config = new CombinedConfiguration();
    sink2Config.addConfiguration(sinkBaseConfig);
    sink2Config.addConfiguration(sink2ConnectionParameters, "connectionParams",
        SimpleConnectionParametersFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink2Config, SINK2_CONFIG);

    // Flume-spout configuration
    BaseConfiguration flumeSpoutConfig = new BaseConfiguration();
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        SimpleLocationServiceFactory.class.getName());
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        SimpleServiceProviderSerialization.class.getName());
    flumeSpoutConfig.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS,
        SimpleEventReceptorFactory.class.getName());
    config.addConfiguration(flumeSpoutConfig, SPOUT_CONFIG);

    testAll();
  }

  /**
   * Integration test with the Dynamic Location Service and the KryoNet
   * Connection API
   * 
   * @throws Exception
   *           If anything went wrong
   */
  @Test
  public void testDynamicLocationServiceWithKryoNet() throws Exception {
    //
    // Flume Configuration
    //

    // Base storm sink configuration
    BaseConfiguration sinkBaseConfig = new BaseConfiguration();
    sinkBaseConfig.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        DynamicLocationServiceFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        KryoNetServiceProviderSerialization.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS,
        KryoNetEventSenderFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS,
        KryoNetConnectionParametersFactory.class.getName());

    // Location Service configuration
    BaseConfiguration locationServiceConfig = new BaseConfiguration();
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.CONNECTION_STRING,
    // zkServer.getConnectString());
        "127.0.0.1:" + ZK_PORT);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.SESSION_TIMEOUT, 2000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.CONNECTION_TIMEOUT, 500);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.RECONNECTION_DELAY, 1000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.TERMINATION_TIMEOUT, 2000);
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.BASE_PATH, "/unitTest");
    locationServiceConfig.addProperty(DynamicLocationServiceConfiguration.SERVICE_NAME, "ut");

    // First storm sink configuration
    BaseConfiguration sink1ConnectionParameters = new BaseConfiguration();
    sink1ConnectionParameters.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    sink1ConnectionParameters.addProperty(KryoNetConnectionParameters.PORT, TestUtils.getAvailablePort());
    CombinedConfiguration sink1Config = new CombinedConfiguration();
    sink1Config.addConfiguration(sinkBaseConfig);
    sink1Config.addConfiguration(sink1ConnectionParameters, "connectionParams",
        KryoNetConnectionParametersFactory.CONFIG_BASE_NAME);
    sink1Config.addConfiguration(locationServiceConfig, "Location Service Configuration",
        DynamicLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink1Config, SINK1_CONFIG);

    // Second storm sink configuration
    BaseConfiguration sink2ConnectionParameters = new BaseConfiguration();
    sink2ConnectionParameters.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    sink2ConnectionParameters.addProperty(KryoNetConnectionParameters.PORT, TestUtils.getAvailablePort());
    CombinedConfiguration sink2Config = new CombinedConfiguration();
    sink2Config.addConfiguration(sinkBaseConfig);
    sink2Config.addConfiguration(sink2ConnectionParameters, "connectionParams",
        KryoNetConnectionParametersFactory.CONFIG_BASE_NAME);
    sink2Config.addConfiguration(locationServiceConfig, "Location Service Configuration",
        DynamicLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink2Config, SINK2_CONFIG);

    //
    // Storm Configuration
    //

    // Global KryoNet configuration
    MapConfiguration kryoConfig = new MapConfiguration(new HashMap<String, Object>());
    kryoConfig.addProperty(KryoNetParameters.CONNECTION_TIMEOUT, 500);
    kryoConfig.addProperty(KryoNetParameters.RECONNECTION_DELAY, 1000);
    kryoConfig.addProperty(KryoNetParameters.TERMINATION_TO, 2000);

    // Flume-spout base configuration
    CombinedConfiguration flumeSpoutBaseConfig = new CombinedConfiguration();
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        DynamicLocationServiceFactory.class.getName());
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        KryoNetServiceProviderSerialization.class.getName());
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS,
        KryoNetEventReceptorFactory.class.getName());

    // Final flume-spout configuration
    CombinedConfiguration flumeSpoutConfig = new CombinedConfiguration();
    flumeSpoutConfig.addConfiguration(flumeSpoutBaseConfig);
    flumeSpoutConfig.addConfiguration(kryoConfig, "Kryo Configuration", KryoNetParameters.CONFIG_BASE_NAME);
    flumeSpoutConfig.addConfiguration(locationServiceConfig, "Location Service Configuration",
        DynamicLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(flumeSpoutConfig, SPOUT_CONFIG);
    testAll();
  }

  public static class KryoNetServiceProvidersLoader implements
      ServiceProviderConfigurationLoader<KryoNetServiceProvider> {
    /**
     * @see com.comcast.viper.flume2storm.location.ServiceProviderConfigurationLoader#load(org.apache.commons.configuration.Configuration)
     */
    @Override
    public KryoNetServiceProvider load(Configuration config) throws F2SConfigurationException {
      return new KryoNetServiceProvider(KryoNetConnectionParameters.from(config));
    }
  }

  /**
   * Integration test with the Dynamic Location Service and the KryoNet
   * Connection API
   * 
   * @throws Exception
   *           If anything went wrong
   */
  @Test
  public void staticLocationService_KryoNet() throws Exception {
    //
    // Flume Configuration
    //

    // Base storm sink configuration
    BaseConfiguration sinkBaseConfig = new BaseConfiguration();
    sinkBaseConfig.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        StaticLocationServiceFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        KryoNetServiceProviderSerialization.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS,
        KryoNetEventSenderFactory.class.getName());
    sinkBaseConfig.addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS,
        KryoNetConnectionParametersFactory.class.getName());

    // Location Service configuration
    BaseConfiguration flumeLocationServiceConfig = new BaseConfiguration();
    flumeLocationServiceConfig.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS,
        KryoNetServiceProvidersLoader.class.getName());

    // First storm sink configuration
    int sink1Port = TestUtils.getAvailablePort();
    BaseConfiguration sink1ConnectionParameters = new BaseConfiguration();
    sink1ConnectionParameters.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    sink1ConnectionParameters.addProperty(KryoNetConnectionParameters.PORT, sink1Port);
    CombinedConfiguration sink1Config = new CombinedConfiguration();
    sink1Config.addConfiguration(sinkBaseConfig);
    sink1Config.addConfiguration(sink1ConnectionParameters, "connectionParams",
        KryoNetConnectionParametersFactory.CONFIG_BASE_NAME);
    sink1Config.addConfiguration(flumeLocationServiceConfig, "Location Service Configuration",
        StaticLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink1Config, SINK1_CONFIG);

    // Second storm sink configuration
    int sink2Port = TestUtils.getAvailablePort();
    BaseConfiguration sink2ConnectionParameters = new BaseConfiguration();
    sink2ConnectionParameters.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    sink2ConnectionParameters.addProperty(KryoNetConnectionParameters.PORT, sink2Port);
    CombinedConfiguration sink2Config = new CombinedConfiguration();
    sink2Config.addConfiguration(sinkBaseConfig);
    sink2Config.addConfiguration(sink2ConnectionParameters, "connectionParams",
        KryoNetConnectionParametersFactory.CONFIG_BASE_NAME);
    sink2Config.addConfiguration(flumeLocationServiceConfig, "Location Service Configuration",
        StaticLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(sink2Config, SINK2_CONFIG);

    //
    // Storm Configuration
    //

    String sp1Id = "sp1Id";
    String sp2Id = "sp2Id";
    BaseConfiguration stormLocationServiceBaseConfig = new BaseConfiguration();
    stormLocationServiceBaseConfig.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS,
        KryoNetServiceProvidersLoader.class.getName());
    stormLocationServiceBaseConfig.addProperty(StaticLocationServiceConfiguration.SERVICE_PROVIDER_LIST,
        StringUtils.join(sp1Id, StaticLocationServiceConfiguration.SERVICE_PROVIDER_LIST_SEPARATOR, sp2Id));
    BaseConfiguration stormLocationServiceSink1Config = new BaseConfiguration();
    stormLocationServiceSink1Config.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    stormLocationServiceSink1Config.addProperty(KryoNetConnectionParameters.PORT, sink1Port);
    BaseConfiguration stormLocationServiceSink2Config = new BaseConfiguration();
    stormLocationServiceSink2Config.addProperty(KryoNetConnectionParameters.ADDRESS,
        KryoNetConnectionParameters.ADDRESS_DEFAULT);
    stormLocationServiceSink2Config.addProperty(KryoNetConnectionParameters.PORT, sink2Port);
    CombinedConfiguration stormLocationServiceConfig = new CombinedConfiguration();
    stormLocationServiceConfig.addConfiguration(stormLocationServiceBaseConfig);
    stormLocationServiceConfig.addConfiguration(stormLocationServiceSink1Config, "sink1",
        StringUtils.join(StaticLocationServiceConfiguration.SERVICE_PROVIDER_BASE_DEFAULT, ".", sp1Id));
    stormLocationServiceConfig.addConfiguration(stormLocationServiceSink2Config, "sink2",
        StringUtils.join(StaticLocationServiceConfiguration.SERVICE_PROVIDER_BASE_DEFAULT, ".", sp2Id));

    // Global KryoNet configuration
    MapConfiguration kryoConfig = new MapConfiguration(new HashMap<String, Object>());
    kryoConfig.addProperty(KryoNetParameters.CONNECTION_TIMEOUT, 500);
    kryoConfig.addProperty(KryoNetParameters.RECONNECTION_DELAY, 1000);
    kryoConfig.addProperty(KryoNetParameters.TERMINATION_TO, 2000);

    // Flume-spout base configuration
    CombinedConfiguration flumeSpoutBaseConfig = new CombinedConfiguration();
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS,
        StaticLocationServiceFactory.class.getName());
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        KryoNetServiceProviderSerialization.class.getName());
    flumeSpoutBaseConfig.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS,
        KryoNetEventReceptorFactory.class.getName());

    // Final flume-spout configuration
    CombinedConfiguration flumeSpoutConfig = new CombinedConfiguration();
    flumeSpoutConfig.addConfiguration(flumeSpoutBaseConfig);
    flumeSpoutConfig.addConfiguration(kryoConfig, "Kryo Configuration", KryoNetParameters.CONFIG_BASE_NAME);
    flumeSpoutConfig.addConfiguration(stormLocationServiceConfig, "Location Service Configuration",
        StaticLocationServiceFactory.CONFIG_BASE_NAME);
    config.addConfiguration(flumeSpoutConfig, SPOUT_CONFIG);

    testAll();
  }

  @SuppressWarnings("unchecked")
  protected static final Context configToContext(Configuration configuration) {
    if (configuration == null) {
      return new Context();
    }
    return new Context(ConfigurationConverter.getMap(configuration));
  }

  private static void dumpConfig(String description, Configuration config) {
    System.out.println(description);
    ConfigurationUtils.dump(config, System.out);
    System.out.println("\n");
  }

  /**
   * This test implements 2 storm-sinks and a topology that uses 2 flume-spout.
   * The topology collects all the events received into a {@link MemoryStorage}.
   * As soon as the topology is started, the test writes the test events into
   * the channel.
   * 
   * @throws Exception
   *           If anything went wrong
   */
  protected <CP extends ConnectionParameters, SP extends ServiceProvider<CP>, ES extends EventSender<CP>> void testAll()
      throws Exception {
    dumpConfig("Integration test with the following first storm-sink configuration: ",
        config.getConfiguration(SINK1_CONFIG));
    dumpConfig("Integration test with the following flume-spout configuration: ", config.getConfiguration(SPOUT_CONFIG));

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
        FlumeSpout<CP, SP> flumeSpout = new FlumeSpout<CP, SP>(eventEmitters, config.getConfiguration(SPOUT_CONFIG));
        builder.setSpout("FlumeSpout", flumeSpout, 2);
        final TestBolt psBolt = new TestBolt();
        builder.setBolt("TestBolt", psBolt, 2).shuffleGrouping("FlumeSpout");

        // Starting topology
        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.registerSerialization(F2SEvent.class, F2SEventSerializer.class);
        conf.setFallBackOnJavaSerialization(false);
        cluster.submitTopology(TEST_TOPOLOGY_NAME, conf, builder.createTopology());

        // Creating Flume Channel
        final PseudoTxnMemoryChannel channel = new PseudoTxnMemoryChannel();
        channel.configure(configToContext(config.getConfiguration(CHANNEL_CONFIG)));

        // Creating Flume sinks
        final StormSink<CP, ES> sink1 = new StormSink<CP, ES>();
        sink1.configure(configToContext(config.getConfiguration(SINK1_CONFIG)));
        sink1.setChannel(channel);

        final StormSink<CP, ES> sink2 = new StormSink<CP, ES>();
        sink2.configure(configToContext(config.getConfiguration(SINK2_CONFIG)));
        sink2.setChannel(channel);

        // Creating the Flume sink runner and processor
        LoadBalancingSinkProcessor sinkProcessor = new LoadBalancingSinkProcessor();
        sinkProcessor.setSinks(ImmutableList.of((Sink) sink1, (Sink) sink2));
        sinkProcessor.configure(configToContext(config.getConfiguration(SINK_PROCESSOR_CONFIG)));
        final SinkRunner sinkRunner = new SinkRunner(sinkProcessor);
        sinkRunner.start();

        // Thread to send the events once both Flume and Storm are ready
        Thread senderThread = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              // Waiting that topology is ready
              LOG.info("Waiting that receptors connect...");
              if (!TestUtils.waitFor(new TestCondition() {
                @Override
                public boolean evaluate() {
                  return sink1.getEventSernderStats().getNbClients() == 2
                      && sink2.getEventSernderStats().getNbClients() == 2;
                }
              }, TEST_TIMEOUT)) {
                Assert.fail("Receptors failed to connect to senders in time (" + TEST_TIMEOUT + " ms)");
              }
              LOG.info("Receptors connected... sending events");

              // Load balancing events between the 2 event senders
              for (F2SEvent event : eventsToSent) {
                channel.put(EventBuilder.withBody(event.getBody(), event.getHeaders()));
              }
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
            return MemoryStorage.getInstance().getReceivedEvents().size() >= NB_EVENTS;
          }
        }, TEST_TIMEOUT)) {
          Assert.fail("Failed to receive all events in time (" + TEST_TIMEOUT + " ms)");
        }

        // Testing results:
        // Programming note: I used SortedSet and iterate over the 2 sets to
        // speed up the comparison
        assertThat(MemoryStorage.getInstance().getReceivedEvents().size()).isEqualTo(NB_EVENTS);
        Iterator<F2SEvent> it1 = eventsToSent.iterator();
        Iterator<F2SEvent> it2 = MemoryStorage.getInstance().getReceivedEvents().iterator();
        while (it1.hasNext() && it2.hasNext()) {
          assertThat(it1.next()).isEqualTo(it2.next());
        }
        // Testing Flume
        for (StormSink<?, ?> sink : new StormSink<?, ?>[] { sink1, sink2 }) {
          assertThat(sink.getEventSernderStats().getNbEventsFailed()).isEqualTo(0);
          assertThat(sink.getEventSernderStats().getNbEventsIn()).isEqualTo(HALF_NB_EVENTS);
          assertThat(sink.getEventSernderStats().getNbEventsOut()).isEqualTo(HALF_NB_EVENTS);
          assertThat(sink.getSinkCounter().getEventDrainAttemptCount()).isEqualTo(HALF_NB_EVENTS);
          assertThat(sink.getSinkCounter().getEventDrainSuccessCount()).isEqualTo(HALF_NB_EVENTS);
        }

        // Stopping topology
        LOG.info("Killing topology...");
        KillOptions killOptions = new KillOptions();
        killOptions.set_wait_secs(5);
        cluster.killTopologyWithOpts(TEST_TOPOLOGY_NAME, killOptions);
        TestUtils.waitFor(new TestCondition() {
          @Override
          public boolean evaluate() {
            try {
              return cluster.getClusterInfo().get_topologies().isEmpty();
            } catch (Exception e) {
              return false;
            }
          }
        }, TEST_TIMEOUT);

        // Stopping Flume components
        LOG.info("Stopping Flume components...");
        sinkRunner.stop();
        TestUtils.waitFor(new TestCondition() {
          @Override
          public boolean evaluate() {
            return sinkRunner.getLifecycleState() == LifecycleState.STOP;
          }
        }, TEST_TIMEOUT);
        LOG.info("Integration test done!");
      }
    });
  }
}
