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
package com.comcast.viper.flume2storm.sender;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Statistics related to the Event Sender, specifically for the KryoNet
 * implementation
 */
public final class KryoNetEventSenderStats {
  protected final AtomicReference<KryoNetEventSenderStatus> status;
  protected final AtomicInteger nbClients;
  protected final AtomicLong nbEventIn;
  protected final AtomicLong nbEventOut;
  protected final AtomicLong nbEventFailed;

  public KryoNetEventSenderStats() {
    status = new AtomicReference<KryoNetEventSenderStatus>(KryoNetEventSenderStatus.STOPPED);
    nbClients = new AtomicInteger();
    nbEventIn = new AtomicLong();
    nbEventOut = new AtomicLong();
    nbEventFailed = new AtomicLong();
  }

  public final KryoNetEventSenderStats reset() {
    nbClients.set(0);
    nbEventIn.set(0);
    nbEventOut.set(0);
    nbEventFailed.set(0);
    return this;
  }

  public final KryoNetEventSenderStatus getStatus() {
    return status.get();
  }

  public final KryoNetEventSenderStats setStatus(KryoNetEventSenderStatus newStatus) {
    Preconditions.checkNotNull(newStatus);
    status.set(newStatus);
    return this;
  }

  public final int getNbClients() {
    return nbClients.get();
  }

  public final KryoNetEventSenderStats incrClients() {
    nbClients.incrementAndGet();
    return this;
  }

  public final KryoNetEventSenderStats decrClients() {
    nbClients.decrementAndGet();
    return this;
  }

  public final KryoNetEventSenderStats resetClients() {
    nbClients.set(0);
    return this;
  }

  public final long getNbEventsIn() {
    return nbEventIn.get();
  }

  public final KryoNetEventSenderStats incrEventsIn() {
    return incrEventsIn(1);
  }

  public final KryoNetEventSenderStats incrEventsIn(final int i) {
    nbEventIn.addAndGet(i);
    return this;
  }

  public final long getNbEventsOut() {
    return nbEventOut.get();
  }

  public final KryoNetEventSenderStats incrEventsOut() {
    return incrEventsOut(1);
  }

  public final KryoNetEventSenderStats incrEventsOut(final int i) {
    nbEventOut.addAndGet(i);
    return this;
  }

  public final long getNbEventsFailed() {
    return nbEventFailed.get();
  }

  public final KryoNetEventSenderStats incrEventsFailed() {
    return incrEventsFailed(1);
  }

  public final KryoNetEventSenderStats incrEventsFailed(final int i) {
    nbEventFailed.addAndGet(i);
    return this;
  }

  /**
   * @param other
   *          Another KryoNet Event Sender statistics object
   * @return True if the stats are the same
   */
  public boolean sameAs(KryoNetEventSenderStats other) {
    Preconditions.checkNotNull(other);
    if (getStatus() != other.getStatus())
      return false;
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
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("Status", getStatus()).add("NbClients", getNbClients())
        .add("NbEventsIn", getNbEventsIn()).add("NbEventsOut", getNbEventsOut())
        .add("NbEventsFailed", getNbEventsFailed()).toString();
  }
}
