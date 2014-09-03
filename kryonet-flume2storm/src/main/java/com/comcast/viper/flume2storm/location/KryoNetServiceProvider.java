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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.comcast.viper.flume2storm.connection.KryoNetConnectionParameters;

/**
 * Implementation of {@link ServiceProvider} for KryoNet
 */
public class KryoNetServiceProvider implements ServiceProvider<KryoNetConnectionParameters> {
  private static final long serialVersionUID = -6694093091877189894L;
  protected final KryoNetConnectionParameters connectionParameters;

  protected KryoNetServiceProvider() {
    connectionParameters = new KryoNetConnectionParameters();
  }

  /**
   * Constructor
   * 
   * @param connectionParameters
   *          See {@link #getConnectionParameters()}t
   */
  public KryoNetServiceProvider(KryoNetConnectionParameters connectionParameters) {
    this.connectionParameters = connectionParameters;
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
  public KryoNetConnectionParameters getConnectionParameters() {
    return connectionParameters;
  }

  /**
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(ServiceProvider<KryoNetConnectionParameters> o) {
    int res = connectionParameters.getServerAddress().compareTo(o.getConnectionParameters().getServerAddress());
    if (res != 0)
      return res;
    int thisPort = connectionParameters.getServerPort();
    int otherPort = o.getConnectionParameters().getServerPort();
    return (thisPort < otherPort ? -1 : (thisPort == otherPort ? 0 : 1));
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(connectionParameters).hashCode();
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
    KryoNetServiceProvider other = (KryoNetServiceProvider) obj;
    return new EqualsBuilder().append(this.connectionParameters, other.connectionParameters).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder("KryoNetServiceProvider").append("connectionParameters", connectionParameters)
        .toString();
  }
}
