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
package com.comcast.viper.flume2storm.zookeeper;

import java.io.Serializable;

import org.apache.commons.configuration.Configuration;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.google.common.base.Preconditions;

/**
 * Configuration for {@link ZkClient}
 */
public class ZkClientConfiguration implements Serializable {
  private static final long serialVersionUID = 4096690012104970344L;
  /** Configuration attribute name for {@link #getConnectionStr()} */
  public static final String CONNECTION_STRING = "connection.string";
  /** Default value for {@value #CONNECTION_STRING} */
  public static final String CONNECTION_STRING_DEFAULT = "localhost:2181";
  /** Configuration attribute name for {@link #getSessionTimeout()} */
  public static final String SESSION_TIMEOUT = "session.timeout";
  /** Default value for {@value #SESSION_TIMEOUT} */
  public static final int SESSION_TIMEOUT_DEFAULT = 30000;
  /** Configuration attribute name for {@link #getConnectionTimeout()} */
  public static final String CONNECTION_TIMEOUT = "connection.timeout";
  /** Default value for {@value #CONNECTION_TIMEOUT} */
  public static final int CONNECTION_TIMEOUT_DEFAULT = 10000;
  /** Configuration attribute name for {@link #getReconnectionDelay()} */
  public static final String RECONNECTION_DELAY = "reconnection.delay.in.ms";
  /** Default value for {@value #RECONNECTION_DELAY} */
  public static final int RECONNECTION_DELAY_DEFAULT = 10000;
  /** Configuration attribute name for {@link #getTerminationTimeout()} */
  public static final String TERMINATION_TIMEOUT = "termination.timeout";
  /** Default value for {@value #TERMINATION_TIMEOUT} */
  public static final int TERMINATION_TIMEOUT_DEFAULT = 10000;

  protected String connectionStr;
  protected int sessionTimeout;
  protected int connectionTimeout;
  protected int reconnectionDelay;
  protected int terminationTimeout;

  /**
   * Builds a new {@link ZkClientConfiguration} based on a Configuration
   * 
   * @param config
   *          The configuration to use
   * @return The newly built {@link ZkClientConfiguration} based on the
   *         configuration specified
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public static ZkClientConfiguration from(Configuration config) throws F2SConfigurationException {
    ZkClientConfiguration result = new ZkClientConfiguration();
    result.connectionStr = config.getString(CONNECTION_STRING, CONNECTION_STRING_DEFAULT);
    result.sessionTimeout = config.getInt(SESSION_TIMEOUT, SESSION_TIMEOUT_DEFAULT);
    result.connectionTimeout = config.getInt(CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT);
    result.reconnectionDelay = config.getInt(RECONNECTION_DELAY, RECONNECTION_DELAY_DEFAULT);
    result.terminationTimeout = config.getInt(TERMINATION_TIMEOUT, TERMINATION_TIMEOUT_DEFAULT);
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public ZkClientConfiguration() {
    connectionStr = CONNECTION_STRING_DEFAULT;
    sessionTimeout = SESSION_TIMEOUT_DEFAULT;
    connectionTimeout = CONNECTION_TIMEOUT_DEFAULT;
    reconnectionDelay = RECONNECTION_DELAY_DEFAULT;
    terminationTimeout = TERMINATION_TIMEOUT_DEFAULT;
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public ZkClientConfiguration(final ZkClientConfiguration other) {
    connectionStr = other.connectionStr;
    sessionTimeout = other.sessionTimeout;
    connectionTimeout = other.connectionTimeout;
    reconnectionDelay = other.reconnectionDelay;
    terminationTimeout = other.terminationTimeout;
  }

  /**
   * @return The Zookeeper connection string (host1:port,host2:port,...)
   */
  public String getConnectionStr() {
    return connectionStr;
  }

  /**
   * @param connectionStr
   *          See {@link #getConnectionStr()}
   */
  public void setConnectionStr(final String connectionStr) {
    // TODO Use a regular expression to validate this is a proper ZooKeeper
    // connection string
    Preconditions.checkArgument(connectionStr.length() > 0, "Zookeeper connection string needs to be set");
    this.connectionStr = connectionStr;
  }

  /**
   * @return The Zookeeper session timeout (in milliseconds). Note: this is
   *         merely a suggestion - the actual session timeout is negotiated
   *         between ZK client and server
   */
  public int getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * @param sessionTimeout
   *          See {@link #getSessionTimeout()}
   */
  public void setSessionTimeout(final int sessionTimeout) {
    Preconditions
        .checkArgument(sessionTimeout > 0, "Zookeeper session timeout needs to be a strictly positive integer");
    this.sessionTimeout = sessionTimeout;
  }

  /**
   * @return The Zookeeper connection timeout (in milliseconds). This is the
   *         maximum time allowed for ZooKeeper client to fully established the
   *         connection with the ZooKeeper server.
   */
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * @param connectionTimeout
   *          See {@link #getSessionTimeout()}
   */
  public void setConnectionTimeout(final int connectionTimeout) {
    Preconditions.checkArgument(connectionTimeout > 0,
        "Zookeeper connection timeout needs to be a strictly positive integer");
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * @return The reconnection delay in milliseconds. That's the time to wait
   *         before retrying to reconnect Zookeeper server in case the session
   *         expired
   */
  public int getReconnectionDelay() {
    return reconnectionDelay;
  }

  /**
   * @param reconnectionDelay
   *          {@link #getReconnectionDelay()}
   */
  public void setReconnectionDelay(final int reconnectionDelay) {
    Preconditions.checkArgument(reconnectionDelay > 0,
        "ZkClient reconnection delay needs to be a strictly positive integer");
    this.reconnectionDelay = reconnectionDelay;
  }

  /**
   * @return The ZkClient termination timeout (in milliseconds)
   */
  public int getTerminationTimeout() {
    return terminationTimeout;
  }

  /**
   * @param terminationTimeout
   *          See {@link #getTerminationTimeout()}
   */
  public void setTerminationTimeout(final int terminationTimeout) {
    Preconditions.checkArgument(terminationTimeout > 0,
        "ZkClient termination timeout needs to be a strictly positive integer");
    this.terminationTimeout = terminationTimeout;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((connectionStr == null) ? 0 : connectionStr.hashCode());
    result = prime * result + connectionTimeout;
    result = prime * result + reconnectionDelay;
    result = prime * result + sessionTimeout;
    result = prime * result + terminationTimeout;
    return result;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final ZkClientConfiguration other = (ZkClientConfiguration) obj;
    if (connectionStr == null) {
      if (other.connectionStr != null) {
        return false;
      }
    } else if (!connectionStr.equals(other.connectionStr)) {
      return false;
    }
    if (connectionTimeout != other.connectionTimeout) {
      return false;
    }
    if (reconnectionDelay != other.reconnectionDelay) {
      return false;
    }
    if (sessionTimeout != other.sessionTimeout) {
      return false;
    }
    if (terminationTimeout != other.terminationTimeout) {
      return false;
    }
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ZkClientConfiguration [connectionStr=" + connectionStr + ", sessionTimeout=" + sessionTimeout
        + ", connectionTimeout=" + connectionTimeout + ", reconnectionDelay=" + reconnectionDelay
        + ", terminationTimeout=" + terminationTimeout + "]";
  }
}
