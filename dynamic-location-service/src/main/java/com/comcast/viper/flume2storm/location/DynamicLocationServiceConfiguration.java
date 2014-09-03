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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.comcast.viper.flume2storm.zookeeper.ZkClient;
import com.comcast.viper.flume2storm.zookeeper.ZkClientConfiguration;
import com.google.common.base.Preconditions;

/**
 * Configuration for {@link ZkClient}
 */
public class DynamicLocationServiceConfiguration extends ZkClientConfiguration {
  private static final long serialVersionUID = 4096690012104970344L;
  /** Configuration attribute name for {@link #getBasePath()} */
  public static final String BASE_PATH = "base.path";
  /** Default value for {@value #BASE_PATH} */
  public static final String BASE_PATH_DEFAULT = "/services";
  /** Configuration attribute name for {@link #getServiceName()} */
  public static final String SERVICE_NAME = "service.name";
  /** Default value for {@value #SERVICE_NAME} */
  public static final String SERVICE_NAME_DEFAULT = "flume2storm";

  protected String basePath;
  protected String serviceName;

  /**
   * @param config
   *          The configuration to use
   * @return The newly built {@link DynamicLocationServiceConfiguration} based
   *         on the configuration specified
   */
  public static DynamicLocationServiceConfiguration from(Configuration config) {
    DynamicLocationServiceConfiguration result = new DynamicLocationServiceConfiguration();
    result.setConnectionStr(config.getString(CONNECTION_STRING, CONNECTION_STRING_DEFAULT));
    result.setSessionTimeout(config.getInt(SESSION_TIMEOUT, SESSION_TIMEOUT_DEFAULT));
    result.setConnectionTimeout(config.getInt(CONNECTION_TIMEOUT, CONNECTION_ITMEOUT_DEFAULT));
    result.setReconnectionDelay(config.getInt(RECONNECTION_DELAY, RECONNECTION_DELAY_DEFAULT));
    result.setTerminationTimeout(config.getInt(TERMINATION_TIMEOUT, TERMINATION_TIMEOUT_DEFAULT));
    result.setBasePath(config.getString(BASE_PATH, BASE_PATH_DEFAULT));
    result.setServiceName(config.getString(SERVICE_NAME, SERVICE_NAME_DEFAULT));
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public DynamicLocationServiceConfiguration() {
    super();
    connectionStr = CONNECTION_STRING_DEFAULT;
    sessionTimeout = SESSION_TIMEOUT_DEFAULT;
    connectionTimeout = CONNECTION_ITMEOUT_DEFAULT;
    reconnectionDelay = RECONNECTION_DELAY_DEFAULT;
    terminationTimeout = TERMINATION_TIMEOUT_DEFAULT;
    basePath = BASE_PATH_DEFAULT;
    serviceName = SERVICE_NAME_DEFAULT;
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public DynamicLocationServiceConfiguration(final DynamicLocationServiceConfiguration other) {
    super(other);
    basePath = other.basePath;
    serviceName = other.serviceName;
  }

  /**
   * @return The service name (which is used to group all the
   *         {@link ServiceProvider} instances in ZooKeeper)
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * @param serviceName
   *          See {@link #getServiceName()}
   */
  public void setServiceName(final String serviceName) {
    // TODO Use a regular expression to validate there are no invalid char
    Preconditions.checkArgument(serviceName.length() > 0, "Flume2Storm service name needs to be set");
    this.serviceName = serviceName;
  }

  /**
   * @return ZooKeeper's root path to store this service
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * @param basePath
   *          See {@link #getBasePath()}
   */
  public void setBasePath(final String basePath) {
    // TODO Use a regular expression to validate there are no invalid char
    this.basePath = basePath;
  }

  /**
   * @see com.comcast.viper.flume2storm.zookeeper.ZkClientConfiguration#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().appendSuper(super.hashCode()).append(basePath).append(serviceName).hashCode();
  }

  /**
   * @see com.comcast.viper.flume2storm.zookeeper.ZkClientConfiguration#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    DynamicLocationServiceConfiguration other = (DynamicLocationServiceConfiguration) obj;
    return new EqualsBuilder().appendSuper(super.equals(obj)).append(this.basePath, other.basePath)
        .append(this.serviceName, other.serviceName).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder("DynamicLocationServiceConfiguration").append("basePath", basePath)
        .append("serviceName", serviceName).append("connectionStr", connectionStr)
        .append("sessionTimeout", sessionTimeout).append("connectionTimeout", connectionTimeout)
        .append("reconnectionDelay", reconnectionDelay).append("terminationTimeout", terminationTimeout).toString();
  }
}
