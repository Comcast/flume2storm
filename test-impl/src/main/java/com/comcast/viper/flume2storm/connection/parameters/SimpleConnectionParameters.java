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
package com.comcast.viper.flume2storm.connection.parameters;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * Simple implementation of {@link ConnectionParameters} for test purpose
 */
public class SimpleConnectionParameters implements ConnectionParameters {
  /** Configuration attribute name for {@link #getHostname()} */
  public static final String HOSTNAME = "hostname";
  /** Default value for {@value #HOSTNAME} */
  public static final String HOSTNAME_DEFAULT = "localhost";
  /** Configuration attribute name for {@link #getPort()} */
  public static final String PORT = "port";
  /** Default value for {@value #PORT} */
  public static final int PORT_DEFAULT = 7000;

  protected String address;
  protected int port;

  /**
   * @param configuration
   *          The configuration to use
   * @return The newly built {@link SimpleConnectionParameters} based on the
   *         configuration specified
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public static SimpleConnectionParameters from(Configuration configuration) throws F2SConfigurationException {
    SimpleConnectionParameters result = new SimpleConnectionParameters();
    try {
      result.setHostname(configuration.getString(HOSTNAME, HOSTNAME_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(HOSTNAME, configuration.getProperty(HOSTNAME), e);
    }
    try {
      result.setPort(configuration.getInt(PORT, PORT_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(PORT, configuration.getProperty(PORT), e);
    }
    return result;
  }

  /**
   * Constructor that initializes the connection parameters using the defaults
   * settings
   */
  public SimpleConnectionParameters() {
    address = HOSTNAME_DEFAULT;
    port = PORT_DEFAULT;
  }

  /**
   * Constructor that initializes the connection parameters using the specified
   * settings
   * 
   * @param serverAddress
   *          See {@link #getHostname()}
   * @param serverPort
   *          See {@link #getPort()}
   */
  public SimpleConnectionParameters(String serverAddress, int serverPort) {
    this.address = serverAddress;
    this.port = serverPort;
  }

  /**
   * Copy constructor
   * 
   * @param copy
   *          Another configuration object for KryoNet connection parameters
   */
  public SimpleConnectionParameters(final SimpleConnectionParameters copy) {
    address = copy.address;
    port = copy.port;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters#getId()
   */
  @Override
  public String getId() {
    return new StringBuilder().append(address).append(":").append(port).toString();
  }

  /**
   * @return Address (or host name) to use for the KryoNet event sender.
   *         Defaults to {@value #HOSTNAME_DEFAULT}
   */
  public String getHostname() {
    return address;
  }

  /**
   * @param address
   *          See {@link #getHostname()}
   * @return This configuration object
   */
  public SimpleConnectionParameters setHostname(final String address) {
    this.address = address;
    return this;
  }

  /**
   * @return Port to use for the KryoNet event sender. Defaults to
   *         {@value #PORT_DEFAULT}
   */
  public int getPort() {
    return port;
  }

  /**
   * @param port
   *          See {@link #getPort()}
   * @return This configuration object
   */
  public SimpleConnectionParameters setPort(final int port) {
    if (port < 0)
      throw new IllegalArgumentException("...");
    this.port = port;
    return this;
  }

  /**
   * @return The connection string (for identification purpose)
   */
  public String getConnectionStr() {
    return address + ":" + port;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(address).append(port).hashCode();
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
    SimpleConnectionParameters other = (SimpleConnectionParameters) obj;
    return new EqualsBuilder().append(this.address, other.address).append(this.port, other.port).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("address", address).append("port", port)
        .build();
  }
}
