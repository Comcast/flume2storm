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
package com.comcast.viper.flume2storm;

import java.io.Serializable;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.lf5.viewer.categoryexplorer.TreeModelAdapter;

import com.google.common.base.Preconditions;

/**
 * Configuration for the KryoNet sender and receiver
 */
public class KryoNetParameters implements Serializable {
  private static final long serialVersionUID = -5348116647457735026L;
  public static final String CONFIG_BASE_NAME = "kryonet";
  public static final String CONNECTION_TIMEOUT = "connection.timeout.in.ms";
  public static final int CONNECTION_TIMEOUT_DEFAULT = 10000;
  public static final String RETRY_SLEEP_DELAY = "retry.sleep.delay.in.ms";
  public static final int RETRY_SLEEP_DELAY_DEFAULT = 50;
  public static final String RECONNECTION_DELAY = "reconnection.delay.in.ms";
  public static final int RECONNECTION_DELAY_DEFAULT = 2000;
  public static final String TERMINATION_TO = "termination.timeout.in.ms";
  public static final int TERMINATION_TO_DEFAULT = 10000;
  public static final String MAX_RETRIES = "max.retries";
  public static final int MAX_RETRIES_DEFAULT = 3;

  protected int connectionTimeout;
  protected int retrySleepDelay;
  protected int reconnectionDelay;
  protected int terminationTimeout;
  protected int maxRetries;

  public static KryoNetParameters from(final Configuration config) {
    KryoNetParameters result = new KryoNetParameters();
    result.connectionTimeout = config.getInt(CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT);
    result.terminationTimeout = config.getInt(TERMINATION_TO, TERMINATION_TO_DEFAULT);
    result.reconnectionDelay = config.getInt(RECONNECTION_DELAY, RECONNECTION_DELAY_DEFAULT);
    result.retrySleepDelay = config.getInt(RETRY_SLEEP_DELAY, RETRY_SLEEP_DELAY_DEFAULT);
    result.maxRetries = config.getInt(MAX_RETRIES, MAX_RETRIES_DEFAULT);
    return result;
  }

  public KryoNetParameters() {
    connectionTimeout = CONNECTION_TIMEOUT_DEFAULT;
    retrySleepDelay = RETRY_SLEEP_DELAY_DEFAULT;
    reconnectionDelay = RECONNECTION_DELAY_DEFAULT;
    terminationTimeout = TERMINATION_TO_DEFAULT;
    maxRetries = MAX_RETRIES_DEFAULT;
  }

  /**
   * @return The timeout for a connection attempt, in milliseconds
   */
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * @param connectionTo
   *          See #getConnectionTimeout()
   */
  public void setConnectionTimeout(int connectionTo) {
    Preconditions.checkArgument(connectionTo > 0, "KryoNet connection timeout must be strictly positive");
    connectionTimeout = connectionTo;
  }

  /**
   * @return The delay allowed for the client to terminate, in milliseconds. It
   *         should be greater than the connection timeout
   */
  public int getTerminationTimeout() {
    return terminationTimeout;
  }

  /**
   * @param terminationTo
   *          See #getTerminationTimeout()
   */
  public void setTerminationTimeout(int terminationTo) {
    Preconditions.checkArgument(terminationTo > 0, "KryoNet termination timeout must be strictly positive");
    terminationTimeout = terminationTo;
  }

  /**
   * @return The time in milliseconds to wait before the client (i.e.
   *         EventReceptor) retries to connect to the server (i.e. EventSender)
   */
  public int getReconnectionDelay() {
    return reconnectionDelay;
  }

  /**
   * @param reconnectionDelay
   *          See #getReconnectionDelay()
   */
  public void setReconnectionDelay(int reconnectionDelay) {
    Preconditions.checkArgument(reconnectionDelay > 0, "KryoNet reconnection delay must be strictly positive");
    this.reconnectionDelay = reconnectionDelay;
  }

  /**
   * @return The time in milliseconds to wait before the server (i.e.
   *         EventSender) retries sending an event to the same client (i.e.
   *         EventReceptor)
   */
  public int getRetrySleepDelay() {
    return retrySleepDelay;
  }

  /**
   * @param retrySleepDelay
   *          See #getRetrySleepDelay()
   */
  public void setRetrySleepDelay(int retrySleepDelay) {
    Preconditions.checkArgument(retrySleepDelay > 0, "KryoNet retry sleep delay must be strictly positive");
    this.retrySleepDelay = retrySleepDelay;
  }

  /**
   * @return The time in milliseconds to wait before the server (i.e.
   *         EventSender) retries sending an event to the same client (i.e.
   *         EventReceptor)
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * @param maxRetries
   *          See #getMaxRetries()
   */
  public void setMaxRetries(int maxRetries) {
    Preconditions.checkArgument(maxRetries > 0, "KryoNet maximum number of retries must be strictly positive");
    this.maxRetries = maxRetries;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + connectionTimeout;
    result = prime * result + maxRetries;
    result = prime * result + reconnectionDelay;
    result = prime * result + retrySleepDelay;
    result = prime * result + terminationTimeout;
    return result;
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
    KryoNetParameters other = (KryoNetParameters) obj;
    if (connectionTimeout != other.connectionTimeout)
      return false;
    if (maxRetries != other.maxRetries)
      return false;
    if (reconnectionDelay != other.reconnectionDelay)
      return false;
    if (retrySleepDelay != other.retrySleepDelay)
      return false;
    if (terminationTimeout != other.terminationTimeout)
      return false;
    return true;
  }
}
