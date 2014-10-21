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

import junit.framework.Assert;

import com.comcast.viper.flume2storm.connection.receptor.KryoNetEventReceptor;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptorStats;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.connection.sender.KryoNetEventSender;
import com.comcast.viper.flume2storm.connection.sender.EventSenderStats;
import com.comcast.viper.flume2storm.connection.sender.KryoNetEventSenderStatus;
import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.google.common.base.Preconditions;

/**
 * Some utility methods for testing KryoNet implementation of Flume2Storm event
 * distribution
 */
public class KryoNetTestUtil {

  public static void checkSenderStatus(final KryoNetEventSender sender, final EventSenderStats expected) {
    Preconditions.checkNotNull(expected);
    Assert.assertTrue("Expected:" + expected + " but got: " + sender.getStats(), expected.sameAs(sender.getStats()));
  }

  public static void checkReceptorStatus(final KryoNetEventReceptor receptor, final EventReceptorStats expected)
      throws InterruptedException {
    Preconditions.checkNotNull(expected);
    // Adding a small delay for the receptor to get the data from the socket
    Thread.sleep(200);
    Assert
        .assertTrue("Expected:" + expected + " but got: " + receptor.getStats(), expected.equals(receptor.getStats()));
  }

  private static class ReceptorConnectedCondition implements TestCondition {
    private final KryoNetEventReceptor receptor;

    public ReceptorConnectedCondition(final KryoNetEventReceptor receptor) {
      this.receptor = receptor;
    }

    @Override
    public boolean evaluate() {
      return receptor.getStats().isConnected();
    }
  }

  /**
   * Waits that the {@link KryoNetEventReceptor} connects to the
   * {@link KryoNetEventSender} for a max of timeout. It throws an assertion
   * failure if not connected after this time
   * 
   * @param receptor
   *          The {@link KryoNetEventReceptor} to monitor
   * @param timeout
   *          Max wait time, in milliseconds
   * @throws InterruptedException
   */
  public static void waitReceptorConnected(final KryoNetEventReceptor receptor, final int timeout)
      throws InterruptedException {
    if (!TestUtils.waitFor(new ReceptorConnectedCondition(receptor), timeout, 50)) {
      Assert.fail("Receptor failed to connect to sender in time (" + timeout + " ms)");
    }
  }

  private static class ReceptorQueuedAtLeastCondition implements TestCondition {
    private final KryoNetEventReceptor receptor;
    private final int nb;

    public ReceptorQueuedAtLeastCondition(final KryoNetEventReceptor receptor, int nb) {
      this.receptor = receptor;
      this.nb = nb;
    }

    @Override
    public boolean evaluate() {
      return receptor.getStats().getNbEventsQueued() >= nb;
    }
  }

  /**
   * Waits that the {@link KryoNetEventReceptor} connects to the
   * {@link KryoNetEventSender} for a max of timeout. It throws an assertion
   * failure if not connected after this time
   * 
   * @param receptor
   *          The {@link KryoNetEventReceptor} to monitor
   * @param timeout
   *          Max wait time, in milliseconds
   * @throws InterruptedException
   */
  public static void waitReceptorQueuedAtLeast(final KryoNetEventReceptor receptor, final int nbEventsQueued,
      final int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new ReceptorQueuedAtLeastCondition(receptor, nbEventsQueued), timeout, 50)) {
      Assert.fail("Receptor failed to queue up " + nbEventsQueued + " events in time (" + timeout + " ms)");
    }
  }

  private static class SenderShutDownCondition implements TestCondition {
    private final KryoNetEventSender sender;

    public SenderShutDownCondition(final KryoNetEventSender sender) {
      this.sender = sender;
    }

    @Override
    public boolean evaluate() {
      return sender.getStatus() == KryoNetEventSenderStatus.STOPPED;
    }
  }

  /**
   * Waits that the {@link EventSender} shuts down for a max of timeout. It
   * throws an assertion failure if not connected after this time
   * 
   * @param sender
   *          The {@link KryoNetEventSender}
   * @param timeout
   *          Max wait time
   * @throws InterruptedException
   */
  public static void waitSenderShutdown(final KryoNetEventSender sender, final int timeout) throws InterruptedException {
    if (!TestUtils.waitFor(new SenderShutDownCondition(sender), timeout, 50)) {
      Assert.fail("Sender failed to shut down in time (" + timeout + " ms)");
    }
  }
}
