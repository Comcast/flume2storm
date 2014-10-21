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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptorTestUtils;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptor;
import com.comcast.viper.flume2storm.connection.sender.EventSenderTestUtils;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventFactory;

/**
 * Integration test for {@link SimpleEventSender} and
 * {@link SimpleEventReceptor}
 */
public class TestSimpleImplIntegration {
  /**
   * Integration test
   * 
   * @throws InterruptedException
   */
  @Test
  public void testSenderReceptor() throws InterruptedException {
    // Creating EventSender
    SimpleConnectionParameters connectionParameters = new SimpleConnectionParameters();
    SimpleEventSender sender = new SimpleEventSender(connectionParameters);
    sender.start();

    // Creating EventReceptor
    SimpleEventReceptor receptor1 = new SimpleEventReceptor(connectionParameters);
    receptor1.start();
    EventReceptorTestUtils.waitConnected(receptor1);

    // Sending events
    List<F2SEvent> events = new ArrayList<F2SEvent>();
    events.add(F2SEventFactory.getInstance().createRandomWithHeaders());
    sender.send(events);

    // Receiving events
    List<F2SEvent> received = receptor1.getEvents();
    assertThat(received).hasSize(events.size());

    // Stopping EventReceptor
    receptor1.stop();
    EventReceptorTestUtils.waitDisconnected(receptor1);

    // Stopping EventSender
    sender.stop();
    EventSenderTestUtils.waitNoReceptor(sender);
  }
}
