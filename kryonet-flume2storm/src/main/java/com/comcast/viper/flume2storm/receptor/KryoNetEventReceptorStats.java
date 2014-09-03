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
package com.comcast.viper.flume2storm.receptor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.comcast.viper.flume2storm.connection.receptor.EventReceptor;
import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * Statistics related to the Event Receptor, specifically for the KryoNet
 * implementation
 */
public final class KryoNetEventReceptorStats {
  protected final AtomicBoolean isConnected;
  protected final AtomicLong nbEventsIn;
  protected final AtomicLong nbEventsQueued;

  /**
   * Default constructor
   */
  public KryoNetEventReceptorStats() {
    isConnected = new AtomicBoolean(false);
    nbEventsIn = new AtomicLong();
    nbEventsQueued = new AtomicLong();
  }

  /**
   * Resets all the metrics
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats reset() {
    isConnected.set(false);
    nbEventsIn.set(0);
    nbEventsQueued.set(0);
    return this;
  }

  /**
   * @return True if the {@link EventReceptor} is connected
   */
  public final boolean isConnected() {
    return isConnected.get();
  }

  /**
   * See {@link #isConnected()}
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats setConnected() {
    isConnected.set(true);
    return this;
  }

  /**
   * See {@link #isConnected()}
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats setDisconnected() {
    isConnected.set(false);
    return this;
  }

  /**
   * @return The total number of {@link F2SEvent} ingested by the
   *         {@link EventReceptor}
   */
  public final long getNbEventsIn() {
    return nbEventsIn.get();
  }

  /**
   * See {@link #getNbEventsIn()}
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats incrEventsIn() {
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
  public final KryoNetEventReceptorStats incrEventsIn(final int i) {
    nbEventsIn.addAndGet(i);
    return this;
  }

  /**
   * @return The number of {@link F2SEvent} currently in the the
   *         {@link EventReceptor} queue
   */
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
  public final KryoNetEventReceptorStats setEventsQueued(final int i) {
    nbEventsQueued.set(i);
    return this;
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats incrEventsQueued() {
    return incrEventsQueued(1);
  }

  /**
   * See {@link #getNbEventsQueued()}
   * 
   * @return This object
   */
  public final KryoNetEventReceptorStats decrEventsQueued() {
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
  public final KryoNetEventReceptorStats incrEventsQueued(final int i) {
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
    KryoNetEventReceptorStats other = (KryoNetEventReceptorStats) obj;
    return new EqualsBuilder().append(this.isConnected(), other.isConnected())
        .append(this.getNbEventsIn(), other.getNbEventsIn())
        .append(this.getNbEventsQueued(), other.getNbEventsQueued()).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder("KryoNetEventReceptorStats").append("isConnected", isConnected())
        .append("NbEventsIn", getNbEventsIn()).append("NbEventsQueued", getNbEventsQueued()).toString();
  }
}
