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
package com.comcast.viper.flume2storm.connection.receptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSender;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSenderRouter;
import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * An implementation of the {@link EventReceptor} for test purpose
 */
public class SimpleEventReceptor implements EventReceptor<SimpleConnectionParameters> {
  protected static final Logger LOG = LoggerFactory.getLogger(SimpleEventReceptor.class);
  protected final Queue<F2SEvent> events;
  protected final SimpleConnectionParameters connectionParameters;
  protected final EventReceptorStats stats;

  /**
   * @param connectionParameters
   *          The connections parameters to use to connect the
   *          {@link EventSender}
   */
  public SimpleEventReceptor(SimpleConnectionParameters connectionParameters) {
    events = new LinkedList<F2SEvent>();
    this.connectionParameters = connectionParameters;
    stats = new EventReceptorStats(connectionParameters.getId());
    LOG.debug("Created with {}", connectionParameters);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getConnectionParameters()
   */
  @Override
  public SimpleConnectionParameters getConnectionParameters() {
    return connectionParameters;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#start()
   */
  @Override
  public boolean start() {
    SimpleEventSender eventSender = SimpleEventSenderRouter.getInstance().get(connectionParameters);
    if (eventSender != null) {
      System.out.println("Connecting to " + eventSender);
      eventSender.connect(this);
      stats.setConnected();
    }
    LOG.info("Receptor of '{}' started and connected to {}", connectionParameters.getId(), eventSender);
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#stop()
   */
  @Override
  public boolean stop() {
    SimpleEventSender eventSender = SimpleEventSenderRouter.getInstance().get(connectionParameters);
    if (eventSender != null) {
      System.out.println("Disconnecting from " + eventSender);
      eventSender.disconnect(this);
      stats.setDisconnected();
    }
    LOG.info("Receptor of '{}' stopped and disconnected from {}", connectionParameters.getId(), eventSender);
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getStats()
   */
  public EventReceptorStats getStats() {
    return stats;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents()
   */
  @Override
  public List<F2SEvent> getEvents() {
    return getEvents(Integer.MAX_VALUE);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents(int)
   */
  @Override
  public List<F2SEvent> getEvents(int maxEvents) {
    final List<F2SEvent> result = new ArrayList<F2SEvent>();
    for (int i = 0; i < maxEvents; i++) {
      final F2SEvent e = events.poll();
      if (e == null) {
        break;
      }
      stats.decrEventsQueued();
      result.add(e);
    }
    return result;
  }

  /**
   * Method called by the {@link EventSender} to simulate sending a Flume2Storm
   * event
   * 
   * @param event
   *          The event to receive
   */
  public void receive(F2SEvent event) {
    stats.incrEventsIn();
    if (events.add(event))
      stats.incrEventsQueued();
    LOG.trace("Receptor of '{}' received an event", connectionParameters.getId());
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("connectionParameters", connectionParameters).append("stats", stats).build();
  }
}