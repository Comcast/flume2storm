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
package com.comcast.viper.flume2storm.location;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.google.common.base.Preconditions;

/**
 * A simple implementation of ServiceProvider for test/example purpose
 */
public class SimpleServiceProvider implements ServiceProvider<SimpleConnectionParameters> {
  private static final long serialVersionUID = 1512839521887743232L;
  private final SimpleConnectionParameters connectionParameters;

  /**
   * @param connectionParameters
   *          See {@link #getConnectionParameters()}
   */
  public SimpleServiceProvider(SimpleConnectionParameters connectionParameters) {
    Preconditions.checkNotNull(connectionParameters);
    this.connectionParameters = connectionParameters;
  }

  /**
   * @param hostname
   *          See {@link #getHostname()}
   * @param port
   *          See {@link #getPort()}
   */
  public SimpleServiceProvider(String hostname, int port) {
    Preconditions.checkNotNull(hostname);
    Preconditions.checkNotNull(port);
    connectionParameters = new SimpleConnectionParameters();
    connectionParameters.setServerAddress(hostname);
    connectionParameters.setServerPort(port);
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProvider#getId()
   */
  @Override
  public String getId() {
    return new StringBuilder().append(connectionParameters.getServerAddress()).append(":")
        .append(connectionParameters.getServerPort()).toString();
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProvider#getConnectionParameters()
   */
  @Override
  public SimpleConnectionParameters getConnectionParameters() {
    return connectionParameters;
  }

  /**
   * @return The hostname or IP address of the service provider
   */
  public String getHostname() {
    return connectionParameters.getServerAddress();
  }

  /**
   * @return The port that the service provider is listening to
   */
  public int getPort() {
    return connectionParameters.getServerPort();
  }

  /**
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(ServiceProvider<SimpleConnectionParameters> o) {
    if (o == null)
      return 1;
    int res = getHostname().compareTo(o.getConnectionParameters().getServerAddress());
    if (res != 0)
      return res;
    return Integer.valueOf(getPort()).compareTo(o.getConnectionParameters().getServerPort());
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((connectionParameters == null) ? 0 : connectionParameters.hashCode());
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
    SimpleServiceProvider other = (SimpleServiceProvider) obj;
    if (connectionParameters == null) {
      if (other.connectionParameters != null)
        return false;
    } else if (!connectionParameters.equals(other.connectionParameters))
      return false;
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "SimpleServiceProvider [hostname=" + getHostname() + ", port=" + getPort() + "]";
  }
}
