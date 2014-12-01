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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.comcast.viper.flume2storm.connection.sender.EventSender;

/**
 * Statistics related to the {@link EventReceptor}. This class is thread-safe.
 */
public final class EventReceptorStats implements EventReceptorStatsMBean {
  protected final String eventSenderId;
  protected final AtomicBoolean isConnected;
  protected final AtomicLong nbEventsIn;
  protected final AtomicLong nbEventsQueued;

  /**
   * Default constructor
   * 
   * @param eventSenderId
   *          The {@link EventSender} identifier
   */
  public EventReceptorStats(String eventSenderId) {
    this.eventSenderId = eventSenderId;
    isConnected = new AtomicBoolean(false);
    nbEventsIn = new AtomicLong();
    nbEventsQueued = new AtomicLong();
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptorStatsMBean#getEventSenderId()
   */
  @Override
  public String getEventSenderId() {
    return eventSenderId;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptorStatsMBean#reset()
   */
  @Override
  public final EventReceptorStats reset() {
    isConnected.set(false);
    nbEventsIn.set(0);
    nbEventsQueued.set(0);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptorStatsMBean#isConnected()
   */
  @Override
  public final boolean isConnected() {
    return isConnected.get();
  }

  /**
   * See {@link #isConnected()}
   * 
   * @return This object
   */
  public final EventReceptorStats setConnected() {
    isConnected.set(true);
    return this;
  }

  /**
   * See {@link #isConnected()}
   * 
   * @return This object
   */
  public final EventReceptorStats setDisconnected() {
    isConnected.set(false);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptorStatsMBean#getNbEventsIn()
   */
  @Override
  public final long getNbEventsIn() {
    return nbEventsIn.get();
  }

  /**
   * See {@link #getNbEventsIn()}
   * 
   * @return This object
   */
  public final EventReceptorStats incrEventsIn() {
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
  public final EventReceptorStats incrEventsIn(final int i) {
    nbEventsIn.addAndGet(i);
    return this;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptorStatsMBean#getNbEventsQueued()
   */
  @Override
  public final long getNbEventsQueued() {
    return nbEventsQueued.get();
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @param i
   *          The number of events to set the queue to
   * @return This object
   */
  public final EventReceptorStats setEventsQueued(final int i) {
    nbEventsQueued.set(i);
    return this;
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @return This object
   */
  public final EventReceptorStats incrEventsQueued() {
    return incrEventsQueued(1);
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @return This object
   */
  public final EventReceptorStats decrEventsQueued() {
    return incrEventsQueued(-1);
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @param i
   *          The number of events to increment
   * 
   * @return This object
   */
  public final EventReceptorStats incrEventsQueued(final int i) {
    nbEventsQueued.addAndGet(i);
    return this;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(isConnected).append(nbEventsIn).append(nbEventsQueued).hashCode();
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
    EventReceptorStats other = (EventReceptorStats) obj;
    return new EqualsBuilder().append(this.isConnected(), other.isConnected())
        .append(this.getNbEventsIn(), other.getNbEventsIn())
        .append(this.getNbEventsQueued(), other.getNbEventsQueued()).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("isConnected", isConnected())
        .append("NbEventsIn", getNbEventsIn()).append("NbEventsQueued", getNbEventsQueued()).toString();
  }
}
