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
package com.comcast.viper.flume2storm.connection;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptorStats;
import com.comcast.viper.flume2storm.connection.receptor.KryoNetEventReceptor;
import com.comcast.viper.flume2storm.connection.sender.EventSenderStats;
import com.comcast.viper.flume2storm.connection.sender.EventSenderTestUtils;
import com.comcast.viper.flume2storm.connection.sender.KryoNetEventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventBuilder;
import com.comcast.viper.flume2storm.event.F2SEventFactory;
import com.comcast.viper.flume2storm.utility.forwarder.TCPForwarder;
import com.comcast.viper.flume2storm.utility.forwarder.TCPForwarderBuilder;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.google.common.collect.ImmutableList;

public class KryoNetIntegrationTest {
  protected static final int TEST_TO = 1000;
  protected static final int RECONNECTION_TO = 1000;
  protected static KryoNetConnectionParameters connectionParameters;
  protected static KryoNetParameters kryoNetParameters;
  protected KryoNetEventSender sender;
  protected EventSenderStats senderStats;
  protected EventReceptorStats receptorStats;

  @BeforeClass
  public static void init() {
    connectionParameters = new KryoNetConnectionParameters();
    connectionParameters.setObjectBufferSize(1024);
    connectionParameters.setWriteBufferSize(5120);
    connectionParameters.setAddress("localhost");
    connectionParameters.setServerPort(TestUtils.getAvailablePort());
    kryoNetParameters = new KryoNetParameters();
    kryoNetParameters.setConnectionTimeout(RECONNECTION_TO);
    kryoNetParameters.setTerminationTimeout(3000);
    kryoNetParameters.setRetrySleepDelay(RECONNECTION_TO);
  }

  @Before
  public void setup() throws Exception {
    sender = new KryoNetEventSender(connectionParameters, kryoNetParameters);
    sender.getStats().reset();
    sender.start();
    senderStats = new EventSenderStats(connectionParameters.getId());
    receptorStats = new EventReceptorStats(connectionParameters.getId());
  }

  @After
  public void teardown() throws IOException, InterruptedException {
    sender.stop();
    KryoNetTestUtil.waitSenderShutdown(sender, TEST_TO);
  }

  protected List<F2SEvent> generateRandomEvents(int nb) {
    List<F2SEvent> result = new ArrayList<F2SEvent>(nb);
    for (int i = 0; i < nb; i++) {
      result.add(F2SEventFactory.getInstance().createRandomWithHeaders());
    }
    return result;
  }

  @Test
  public void testClientReconnection() throws Exception {
    // Starting client
    KryoNetEventReceptor receptor = new KryoNetEventReceptor(connectionParameters, kryoNetParameters);
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected());

    // Sending the events
    final F2SEvent e1 = F2SEventFactory.getInstance().createRandomWithHeaders();
    final F2SEvent e2 = F2SEventFactory.getInstance().createRandomWithHeaders();
    final F2SEvent e3 = F2SEventFactory.getInstance().createRandomWithHeaders();
    sender.send(ImmutableList.of(e1, e2, e3));

    // Waiting that all the events have been sent
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn(3).incrEventsOut(3));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn(3).incrEventsQueued(3));

    List<F2SEvent> events = receptor.getEvents();
    Assert.assertEquals(3, events.size());
    Assert.assertEquals(e1, events.get(0));
    Assert.assertEquals(e2, events.get(1));
    Assert.assertEquals(e3, events.get(2));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Disconnecting client
    receptor.stop();
    EventSenderTestUtils.waitNoReceptor(sender, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.decrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setDisconnected());

    // Sending some more events - queued up
    Assert.assertEquals(0, sender.send(generateRandomEvents(2)));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats);

    // Re-connecting client
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    Assert.assertEquals(2, sender.send(generateRandomEvents(2)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients().incrEventsIn(2).incrEventsOut(2));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.reset().setConnected().incrEventsIn(2)
        .incrEventsQueued(2));

    Assert.assertEquals(2, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Disconnecting client
    receptor.stop();
  }

  /**
   * Testing sending an event that is larger than the object buffer size. When
   * doing so, the sender sends it successfully, but the receptor will generate
   * an exception and then disconnect. The {@link KryoNetEventReceptor}
   * implementation will reconnect right away.
   * 
   * @throws Exception
   */
  @Test
  public void testMessageTooBig() throws Exception {
    final byte[] prettyBig = new byte[connectionParameters.getObjectBufferSize() + 512];
    for (int i = 0; i < prettyBig.length; i++) {
      final int mod = i % 10;
      prettyBig[i] = mod == 9 ? (byte) '-' : (byte) ('1' + mod);
    }
    final F2SEvent e1 = new F2SEventBuilder().body(prettyBig).get();

    KryoNetEventReceptor receptor = new KryoNetEventReceptor(connectionParameters, kryoNetParameters);
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected());

    Assert.assertEquals(1, sender.send(ImmutableList.of(e1)));
    // Reconnect should be immediate, but the nb of received messages won't incr
    KryoNetTestUtil.waitReceptorConnected(receptor, RECONNECTION_TO + TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn().incrEventsOut());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats);

    final F2SEvent e2 = F2SEventFactory.getInstance().createRandomWithHeaders();
    Assert.assertEquals(1, sender.send(ImmutableList.of(e2)));
    KryoNetTestUtil.waitReceptorQueuedAtLeast(receptor, 1, TEST_TO);
    List<F2SEvent> receivedEvents = receptor.getEvents();
    Assert.assertEquals(1, receivedEvents.size());
    Assert.assertEquals(e2, receivedEvents.get(0));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn().incrEventsOut());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn());

    receptor.stop();
    EventSenderTestUtils.waitNoReceptor(sender, TEST_TO);
  }

  /**
   * Test that the buffer underflow exception is not thrown
   * 
   * @throws Exception
   *           If anything went wrong
   */
  @Test
  public void testBufferUnderflow() throws Exception {
    int fairlyBig = 512 + 20;
    final byte[] prettyBig = new byte[fairlyBig];
    for (int i = 0; i < prettyBig.length; i++) {
      final int mod = i % 10;
      prettyBig[i] = mod == 9 ? (byte) '-' : (byte) ('1' + mod);
    }
    final F2SEvent e1 = new F2SEventBuilder().body(prettyBig).get();

    Assert.assertTrue(fairlyBig > 512 && fairlyBig < connectionParameters.getObjectBufferSize());
    KryoNetEventReceptor receptor = new KryoNetEventReceptor(connectionParameters, kryoNetParameters);
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected());

    Assert.assertEquals(1, sender.send(ImmutableList.of(e1)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn().incrEventsOut());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn().incrEventsQueued());

    receptor.stop();
    EventSenderTestUtils.waitNoReceptor(sender, TEST_TO);
  }

  @Test
  public void testServerRestart() throws Exception {
    // Starting client
    KryoNetEventReceptor receptor = new KryoNetEventReceptor(connectionParameters, kryoNetParameters);
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected());

    // Sending the events
    Assert.assertEquals(3, sender.send(generateRandomEvents(3)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn(3).incrEventsOut(3));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn(3).incrEventsQueued(3));

    Assert.assertEquals(3, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Stopping server
    sender.stop();
    KryoNetTestUtil.waitSenderShutdown(sender, TEST_TO);
    Thread.sleep(4000);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.decrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setDisconnected());

    // Sending some more events
    Assert.assertEquals(0, sender.send(generateRandomEvents(3)));

    // Starting server
    sender.getStats().reset();
    sender.start();

    KryoNetTestUtil.waitReceptorConnected(receptor, RECONNECTION_TO + TEST_TO);
    Assert.assertEquals(5, sender.send(generateRandomEvents(5)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.reset().incrClients().incrEventsIn(5).incrEventsOut(5));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected().incrEventsIn(5).incrEventsQueued(5));

    Assert.assertEquals(5, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Disconnecting client
    receptor.stop();
  }

  @Test
  public void testBadConnection() throws Exception {
    // Setting up proxy
    final String proxyHost = "127.0.0.1";
    final int proxyPort = TestUtils.getAvailablePort();
    final TCPForwarderBuilder tcpForwarderBuilder = new TCPForwarderBuilder();
    tcpForwarderBuilder.setListenAddress(proxyHost).setInputPort(proxyPort)
        .setOutputServer(connectionParameters.getAddress()).setOutputPort(connectionParameters.getPort());
    final TCPForwarder forwarder = tcpForwarderBuilder.build();
    forwarder.start();
    TestUtils.waitFor(new TestCondition() {
      @Override
      public boolean evaluate() {
        return forwarder.isActive();
      }
    }, TEST_TO);

    // Starting client
    connectionParameters.setServerPort(proxyPort);
    KryoNetEventReceptor receptor = new KryoNetEventReceptor(connectionParameters, kryoNetParameters);
    receptor.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected());

    // Sending the events
    Assert.assertEquals(3, sender.send(generateRandomEvents(3)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn(3).incrEventsOut(3));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn(3).incrEventsQueued(3));

    Assert.assertEquals(3, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Screwing up with the connection (short amount of time)
    forwarder.freeze();
    Thread.sleep(RECONNECTION_TO / 2);

    // Sending some more events
    Assert.assertEquals(2, sender.send(generateRandomEvents(2)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrEventsIn(2).incrEventsOut(2));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats);

    // Re-establishing connection
    forwarder.resume();
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.incrEventsIn(2).incrEventsQueued(2));

    Assert.assertEquals(2, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Screwing up with the connection some more
    forwarder.stop();
    EventSenderTestUtils.waitNoReceptor(sender, TEST_TO);
    Assert.assertEquals(0, sender.send(generateRandomEvents(2)));
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.decrClients());
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setDisconnected());
    Thread.sleep(TEST_TO);

    // Re-establishing connection
    forwarder.start();
    KryoNetTestUtil.waitReceptorConnected(receptor, TEST_TO);

    Assert.assertEquals(2, sender.send(generateRandomEvents(2)));
    KryoNetTestUtil.waitReceptorQueuedAtLeast(receptor, 2, TEST_TO);
    KryoNetTestUtil.checkSenderStatus(sender, senderStats.incrClients().incrEventsIn(2).incrEventsOut(2));
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setConnected().incrEventsIn(2).incrEventsQueued(2));

    Assert.assertEquals(2, receptor.getEvents().size());
    KryoNetTestUtil.checkSenderStatus(sender, senderStats);
    KryoNetTestUtil.checkReceptorStatus(receptor, receptorStats.setEventsQueued(0));

    // Disconnecting client
    receptor.stop();
    forwarder.stop();
  }
}