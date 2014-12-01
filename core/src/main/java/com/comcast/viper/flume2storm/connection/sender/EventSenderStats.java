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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.base.Preconditions;

/**
 * Statistics related to the {@link EventSender}. This class is thread-safe.
 */
public final class EventSenderStats implements EventSenderStatsMBean {
  protected final String eventSenderId;
  protected final AtomicInteger nbClients;
  protected final AtomicLong nbEventIn;
  protected final AtomicLong nbEventOut;
  protected final AtomicLong nbEventFailed;

  /**
   * Default constructor
   * 
   * @param eventSenderId
   *          The {@link EventSender} identifier
   */
  public EventSenderStats(final String eventSenderId) {
    this.eventSenderId = eventSenderId;
    nbClients = new AtomicInteger();
    nbEventIn = new AtomicLong();
    nbEventOut = new AtomicLong();
    nbEventFailed = new AtomicLong();
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#getEventSenderId()
   */
  @Override
  public String getEventSenderId() {
    return eventSenderId;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#reset()
   */
  @Override
  public final EventSenderStats reset() {
    nbClients.set(0);
    nbEventIn.set(0);
    nbEventOut.set(0);
    nbEventFailed.set(0);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#getNbClients()
   */
  @Override
  public final int getNbClients() {
    return nbClients.get();
  }

  /**
   * See {@link #getNbClients()}
   * 
   * @return This object
   */
  public final EventSenderStats incrClients() {
    nbClients.incrementAndGet();
    return this;
  }

  /**
   * See {@link #getNbClients()}
   * 
   * @return This object
   */
  public final EventSenderStats decrClients() {
    nbClients.decrementAndGet();
    return this;
  }

  /**
   * See {@link #getNbClients()}
   * 
   * @return This object
   */
  public final EventSenderStatsMBean resetClients() {
    nbClients.set(0);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#getNbEventsIn()
   */
  @Override
  public final long getNbEventsIn() {
    return nbEventIn.get();
  }

  /**
   * See {@link #getNbEventsIn()}
   * 
   * @return This object
   */
  public final EventSenderStats incrEventsIn() {
    return incrEventsIn(1);
  }

  /**
   * See {@link #getNbEventsIn()}
   * 
   * @param i
   *          The number of events to increment
   * 
   * @return This object
   */
  public final EventSenderStats incrEventsIn(final int i) {
    nbEventIn.addAndGet(i);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#getNbEventsOut()
   */
  @Override
  public final long getNbEventsOut() {
    return nbEventOut.get();
  }

  /**
   * See {@link #getNbEventsOut()}
   * 
   * @return This object
   */
  public final EventSenderStats incrEventsOut() {
    return incrEventsOut(1);
  }

  /**
   * See {@link #getNbEventsOut()}
   * 
   * @param i
   *          The number of events to increment
   * 
   * @return This object
   */
  public final EventSenderStats incrEventsOut(final int i) {
    nbEventOut.addAndGet(i);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSenderStatsMBean#getNbEventsFailed()
   */
  @Override
  public final long getNbEventsFailed() {
    return nbEventFailed.get();
  }

  /**
   * See {@link #getNbEventsFailed()}
   * 
   * @return This object
   */
  public final EventSenderStatsMBean incrEventsFailed() {
    return incrEventsFailed(1);
  }

  /**
   * See {@link #getNbEventsFailed()}
   * 
   * @param i
   *          The number of events to increment
   * 
   * @return This object
   */
  public final EventSenderStatsMBean incrEventsFailed(final int i) {
    nbEventFailed.addAndGet(i);
    return this;
  }

  /**
   * @param other
   *          Another KryoNet Event Sender statistics object
   * @return True if the stats are the same
   */
  public boolean sameAs(EventSenderStats other) {
    Preconditions.checkNotNull(other);
    if (getNbClients() != other.getNbClients())
      return false;
    if (getNbEventsIn() != other.getNbEventsIn())
      return false;
    if (getNbEventsOut() != other.getNbEventsOut())
      return false;
    if (getNbEventsFailed() != other.getNbEventsFailed())
      return false;
    return true;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(nbClients).append(nbEventIn).append(nbEventOut).append(nbEventFailed)
        .hashCode();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EventSenderStats other = (EventSenderStats) obj;
    return new EqualsBuilder().append(this.nbClients, other.nbClients).append(this.nbEventIn, other.nbEventIn)
        .append(this.nbEventOut, other.nbEventOut).append(this.nbEventFailed, other.nbEventFailed).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("NbClients", getNbClients())
        .append("NbEventsIn", getNbEventsIn()).append("NbEventsOut", getNbEventsOut())
        .append("NbEventsFailed", getNbEventsFailed()).toString();
  }
}
