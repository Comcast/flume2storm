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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.esotericsoftware.kryonet.Connection;
import com.google.common.base.Preconditions;

/**
 * This is a single-thread strategy that load-balances the events to send
 * between each client. If a client fail to receive the event, it tries another
 * client (for a configurable amount of retries).
 */
public class KryoNetSimpleRealtimeStrategy implements KryoNetEventSenderStrategy, Serializable {
  private static final long serialVersionUID = 3735055103606316628L;
  private static final Logger LOG = LoggerFactory.getLogger(KryoNetSimpleRealtimeStrategy.class);
  private final KryoNetEventSender knEventSender;
  private final int maxRetries;

  /**
   * @param knEventSender
   *          The KryoNet event sender associated with this strategy
   *          implementation
   * @param maxRetries
   *          The maximum number of times the event sender will attempt to send
   *          an event before giving up
   */
  public KryoNetSimpleRealtimeStrategy(KryoNetEventSender knEventSender, final int maxRetries) {
    this.knEventSender = knEventSender;
    this.maxRetries = maxRetries;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.KryoNetEventSenderStrategy#send(java.util.List)
   */
  @Override
  public int send(List<F2SEvent> events) {
    Preconditions.checkNotNull(events);
    int result = 0;
    int retries = 0;
    F2SEvent eventToSend = null;
    Iterator<F2SEvent> it = events.iterator();
    while (knEventSender.getStatus().isOn() && (it.hasNext() || eventToSend != null)) {
      try {
        // Getting the next event to send if necessary
        if (eventToSend == null) {
          eventToSend = it.next();
          if (eventToSend != null) {
            // Got a new event from the generator
            knEventSender.getStats().incrEventsIn();
            retries = 0;
          } else {
            continue;
          }
        }
        assert eventToSend != null;

        // Getting next client
        Connection client = knEventSender.getClients().getNext();
        if (client == null) {
          // No more client
          break;
        }
        LOG.trace("Sending: {} to {}", eventToSend, client);
        if (client.sendTCP(eventToSend) == 0) {
          LOG.debug("Failed to send {} to {}", eventToSend, client);
          retries++;
          if (retries > maxRetries) {
            LOG.debug("Failed to send {} after {} retries", eventToSend, maxRetries);
            knEventSender.getStats().incrEventsFailed();
            eventToSend = null;
          }
        } else {
          // Event sent successfully
          knEventSender.getStats().incrEventsOut();
          result++;
          LOG.trace("Sent {} to {}", eventToSend, client);
          eventToSend = null;
        }
      } catch (final Exception e) {
        StringBuilder sb = new StringBuilder("Failed to send batch of events (");
        sb.append(result);
        sb.append(" sent out of ");
        sb.append(events.size());
        sb.append("): ");
        sb.append(e.getLocalizedMessage());
        LOG.error(sb.toString(), e);
      }
    }
    return result;
  }
}