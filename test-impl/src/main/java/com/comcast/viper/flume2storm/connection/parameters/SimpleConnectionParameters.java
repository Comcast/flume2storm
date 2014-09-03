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

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * Simple configuration for a Flume2Storm connection
 */
public class SimpleConnectionParameters implements ConnectionParameters {
  public static final String HOSTNAME = "hostname";
  public static final String HOSTNAME_DEFAULT = "localhost";
  public static final String PORT = "hostname";
  public static final int PORT_DEFAULT = 7000;

  protected String serverAddress;
  protected int serverPort;

  public static SimpleConnectionParameters from(Configuration configuration) throws F2SConfigurationException {
    SimpleConnectionParameters result = new SimpleConnectionParameters();
    try {
      result.setServerAddress(configuration.getString(HOSTNAME, HOSTNAME_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(HOSTNAME, configuration.getProperty(HOSTNAME), e);
    }
    try {
      result.setServerPort(configuration.getInt(PORT, PORT_DEFAULT));
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
    serverAddress = HOSTNAME_DEFAULT;
    serverPort = PORT_DEFAULT;
  }

  public SimpleConnectionParameters(String serverAddress, int serverPort) {
    this.serverAddress = serverAddress;
    this.serverPort = serverPort;
  }

  /**
   * Copy constructor
   * 
   * @param copy
   *          Another configuration object for KryoNet connection parameters
   */
  public SimpleConnectionParameters(final SimpleConnectionParameters copy) {
    serverAddress = copy.serverAddress;
    serverPort = copy.serverPort;
  }

  /**
   * @return Address (or host name) to use for the KryoNet event sender.
   *         Defaults to {@value #HOSTNAME_DEFAULT}
   */
  public String getServerAddress() {
    return serverAddress;
  }

  /**
   * @param address
   *          See {@link #getServerAddress()}
   * @return This configuration object
   */
  public void setServerAddress(final String address) {
    serverAddress = address;
  }

  /**
   * @return Port to use for the KryoNet event sender. Defaults to
   *         {@value #PORT_DEFAULT}
   */
  public int getServerPort() {
    return serverPort;
  }

  /**
   * @param port
   *          See {@link #getServerPort()}
   * @return This configuration object
   */
  public void setServerPort(final int port) {
    if (port < 0)
      throw new IllegalArgumentException("...");
    serverPort = port;
  }

  /**
   * @return The connection string (for identification purpose)
   */
  public String getConnectionStr() {
    return serverAddress + ":" + serverPort;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((serverAddress == null) ? 0 : serverAddress.hashCode());
    result = prime * result + serverPort;
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
    SimpleConnectionParameters other = (SimpleConnectionParameters) obj;
    if (serverAddress == null) {
      if (other.serverAddress != null)
        return false;
    } else if (!serverAddress.equals(other.serverAddress))
      return false;
    if (serverPort != other.serverPort)
      return false;
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SimpleConnectionParameters [serverAddress=" + serverAddress + ", serverPort=" + serverPort + "]";
  }
}
