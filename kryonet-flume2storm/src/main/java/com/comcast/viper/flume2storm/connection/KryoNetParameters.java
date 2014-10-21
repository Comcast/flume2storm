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

import java.io.Serializable;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.google.common.base.Preconditions;

/**
 * Configuration for the KryoNet sender and receiver
 */
public class KryoNetParameters implements Serializable {
  private static final long serialVersionUID = -5348116647457735026L;
  /** Configuration attribute base name */
  public static final String CONFIG_BASE_NAME = "kryonet";
  /** Configuration attribute name for {@link #getConnectionTimeout()} */
  public static final String CONNECTION_TIMEOUT = "connection.timeout.in.ms";
  /** Default value for {@value #CONNECTION_TIMEOUT} */
  public static final int CONNECTION_TIMEOUT_DEFAULT = 10000;
  /** Configuration attribute name for {@link #getRetrySleepDelay()} */
  public static final String RETRY_SLEEP_DELAY = "retry.sleep.delay.in.ms";
  /** Default value for {@value #RETRY_SLEEP_DELAY} */
  public static final int RETRY_SLEEP_DELAY_DEFAULT = 50;
  /** Configuration attribute name for {@link #getReconnectionDelay()} */
  public static final String RECONNECTION_DELAY = "reconnection.delay.in.ms";
  /** Default value for {@value #RECONNECTION_DELAY} */
  public static final int RECONNECTION_DELAY_DEFAULT = 2000;
  /** Configuration attribute name for {@link #getTerminationTimeout()} */
  public static final String TERMINATION_TO = "termination.timeout.in.ms";
  /** Default value for {@value #TERMINATION_TO} */
  public static final int TERMINATION_TO_DEFAULT = 10000;
  /** Configuration attribute name for {@link #getMaxRetries()} */
  public static final String MAX_RETRIES = "max.retries";
  /** Default value for {@value #MAX_RETRIES} */
  public static final int MAX_RETRIES_DEFAULT = 3;

  protected int connectionTimeout;
  protected int retrySleepDelay;
  protected int reconnectionDelay;
  protected int terminationTimeout;
  protected int maxRetries;

  /**
   * Builds a new {@link KryoNetParameters} based on a Configuration
   * 
   * @param config
   *          The configuration to use
   * @return The newly created {@link F2SConfiguration}
   * @throws F2SConfigurationException
   *           If the configuration specified is invalid
   */
  public static KryoNetParameters from(final Configuration config) throws F2SConfigurationException {
    KryoNetParameters result = new KryoNetParameters();
    try {
      result.setConnectionTimeout(config.getInt(CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, CONNECTION_TIMEOUT, e);
    }
    try {
      result.setTerminationTimeout(config.getInt(TERMINATION_TO, TERMINATION_TO_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, TERMINATION_TO, e);
    }
    try {
      result.setReconnectionDelay(config.getInt(RECONNECTION_DELAY, RECONNECTION_DELAY_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, RECONNECTION_DELAY, e);
    }
    try {
      result.setRetrySleepDelay(config.getInt(RETRY_SLEEP_DELAY, RETRY_SLEEP_DELAY_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, RETRY_SLEEP_DELAY, e);
    }
    try {
      result.setMaxRetries(config.getInt(MAX_RETRIES, MAX_RETRIES_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, MAX_RETRIES, e);
    }
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
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
   * @return This object
   */
  public KryoNetParameters setConnectionTimeout(int connectionTo) {
    Preconditions.checkArgument(connectionTo > 0, "KryoNet connection timeout must be strictly positive");
    connectionTimeout = connectionTo;
    return this;
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
   * @return This object
   */
  public KryoNetParameters setTerminationTimeout(int terminationTo) {
    Preconditions.checkArgument(terminationTo > 0, "KryoNet termination timeout must be strictly positive");
    terminationTimeout = terminationTo;
    return this;
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
   * @return This object
   */
  public KryoNetParameters setReconnectionDelay(int reconnectionDelay) {
    Preconditions.checkArgument(reconnectionDelay > 0, "KryoNet reconnection delay must be strictly positive");
    this.reconnectionDelay = reconnectionDelay;
    return this;
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
   * @return This object
   */
  public KryoNetParameters setRetrySleepDelay(int retrySleepDelay) {
    Preconditions.checkArgument(retrySleepDelay > 0, "KryoNet retry sleep delay must be strictly positive");
    this.retrySleepDelay = retrySleepDelay;
    return this;
  }

  /**
   * @return The maximum number of times an event is attempted to be sent before
   *         considering it failed
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * @param maxRetries
   *          See #getMaxRetries()
   * @return This object
   */
  public KryoNetParameters setMaxRetries(int maxRetries) {
    Preconditions.checkArgument(maxRetries > 0, "KryoNet maximum number of retries must be strictly positive");
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(connectionTimeout).append(retrySleepDelay).append(reconnectionDelay)
        .append(terminationTimeout).append(maxRetries).hashCode();
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
    return new EqualsBuilder().append(this.connectionTimeout, other.connectionTimeout)
        .append(this.retrySleepDelay, other.retrySleepDelay).append(this.reconnectionDelay, other.reconnectionDelay)
        .append(this.terminationTimeout, other.terminationTimeout).append(this.maxRetries, other.maxRetries).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("connectionTimeout", connectionTimeout)
        .append("retrySleepDelay", retrySleepDelay).append("reconnectionDelay", reconnectionDelay)
        .append("terminationTimeout", terminationTimeout).append("maxRetries", maxRetries).toString();
  }
}
