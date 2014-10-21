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
package com.comcast.viper.flume2storm.connection.sender;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptor;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.SimpleServiceProvider;
import com.comcast.viper.flume2storm.utility.circular.CircularList;
import com.comcast.viper.flume2storm.utility.circular.ReadWriteCircularList;

/**
 * A simple implementation of {@link EventSender} for test purpose
 */
public class SimpleEventSender extends SimpleServiceProvider implements EventSender<SimpleConnectionParameters> {
  private static final long serialVersionUID = -6829876645658796790L;
  protected static final Logger LOG = LoggerFactory.getLogger(SimpleEventSender.class);
  protected final CircularList<SimpleEventReceptor> receptors;
  protected final EventSenderStats stats;

  /**
   * @param connectionParameters
   *          See {@link #getConnectionParameters()}
   */
  public SimpleEventSender(SimpleConnectionParameters connectionParameters) {
    super(connectionParameters);
    receptors = new ReadWriteCircularList<SimpleEventReceptor>();
    stats = new EventSenderStats(connectionParameters.getId());
    LOG.debug("Created with {}", connectionParameters);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#start()
   */
  @Override
  public boolean start() {
    LOG.info("Sender '{}' started", getConnectionParameters().getId());
    SimpleEventSenderRouter.getInstance().add(this);
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#stop()
   */
  @Override
  public boolean stop() {
    LOG.info("Sender '{}' stopped", getConnectionParameters().getId());
    SimpleEventSenderRouter.getInstance().remove(this);
    return true;
  }

  /**
   * Method called by the SimpleEventReceptor to connect the EventSender
   * 
   * @param receptor
   *          The receptor that is connecting
   */
  public void connect(SimpleEventReceptor receptor) {
    LOG.info("Sender '{}' received connection from {}", getConnectionParameters().getId(), receptor);
    if (receptors.add(receptor))
      stats.incrClients();
  }

  /**
   * Method called by the SimpleEventReceptor to disconnect the EventSender
   * 
   * @param receptor
   *          The receptor that is connecting
   */
  public void disconnect(SimpleEventReceptor receptor) {
    LOG.info("Sender '{}' received disconnection from {}", getConnectionParameters().getId(), receptor);
    if (receptors.remove(receptor))
      stats.decrClients();
  }

  /**
   * Load-balances the events to send between the receptor connected
   * 
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#send(java.util.List)
   */
  @Override
  public int send(List<F2SEvent> events) {
    LOG.trace("Sender '{}' sending {} events", getConnectionParameters().getId(), events.size());
    stats.incrEventsIn(events.size());
    int result = 0;
    for (F2SEvent f2sEvent : events) {
      SimpleEventReceptor receptor = receptors.getNext();
      if (receptor == null) {
        // No more receptor connected
        break;
      }
      receptor.receive(f2sEvent);
      result++;
    }
    stats.incrEventsOut(result);
    LOG.debug("Sender '{}' sent {} events successfully", getConnectionParameters().getId(), result);
    return result;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#getStats()
   */
  public EventSenderStats getStats() {
    return stats;
  }

  /**
   * @see com.comcast.viper.flume2storm.location.SimpleServiceProvider#toString()
   */
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("connectionParameters", getConnectionParameters()).append("stats", stats).build();
  }
}