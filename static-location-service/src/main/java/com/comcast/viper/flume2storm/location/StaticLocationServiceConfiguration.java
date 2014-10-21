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
import org.apache.commons.lang3.builder.ToStringStyle;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.google.common.base.Preconditions;

/**
 * Configuration for {@link StaticLocationService}
 */
public class StaticLocationServiceConfiguration {
  /** Configuration attribute name to get the list of service providers */
  public static final String SERVICE_PROVIDER_LIST = "service.providers";
  /** The {@link ServiceProvider} list separator */
  public static final String SERVICE_PROVIDER_LIST_SEPARATOR = " ";
  /** Configuration attribute name for {@link #getServiceProviderBase()} */
  public static final String SERVICE_PROVIDER_BASE = "service.providers.base";
  /** Default value for {@value #SERVICE_PROVIDER_BASE} */
  public static final String SERVICE_PROVIDER_BASE_DEFAULT = "service.providers";
  /**
   * Configuration attribute name for the
   * {@link ServiceProviderConfigurationLoader} class name
   */
  public static final String CONFIGURATION_LOADER_CLASS = "configuration.loader.class";

  protected String serviceProviderBase;
  private String configurationLoaderClassName;

  /**
   * @param config
   *          The configuration to use
   * @return The newly built {@link StaticLocationServiceConfiguration} based on
   *         the configuration specified
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public static StaticLocationServiceConfiguration from(Configuration config) throws F2SConfigurationException {
    StaticLocationServiceConfiguration result = new StaticLocationServiceConfiguration();
    result.setServiceProviderBase(config.getString(SERVICE_PROVIDER_BASE, SERVICE_PROVIDER_LIST));
    String className = config.getString(CONFIGURATION_LOADER_CLASS);
    if (className == null) {
      throw F2SConfigurationException.missing(CONFIGURATION_LOADER_CLASS);
    }
    try {
      result.setConfigurationLoaderClassName(className);
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, CONFIGURATION_LOADER_CLASS, e);
    }
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public StaticLocationServiceConfiguration() {
    serviceProviderBase = SERVICE_PROVIDER_LIST;
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public StaticLocationServiceConfiguration(final StaticLocationServiceConfiguration other) {
    serviceProviderBase = other.serviceProviderBase;
    configurationLoaderClassName = other.configurationLoaderClassName;
  }

  /**
   * @return The base name for the {@link ServiceProvider} attributes
   */
  public String getServiceProviderBase() {
    return serviceProviderBase;
  }

  /**
   * @param serviceProviderBase
   *          See {@link #getServiceProviderBase()}
   */
  public void setServiceProviderBase(final String serviceProviderBase) {
    Preconditions.checkNotNull(serviceProviderBase);
    this.serviceProviderBase = serviceProviderBase;
  }

  /**
   * @return The class name of the {@link ServiceProviderConfigurationLoader}
   */
  public String getConfigurationLoaderClassName() {
    return configurationLoaderClassName;
  }

  /**
   * @param configurationLoaderClassName
   *          See {@link #getConfigurationLoaderClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setConfigurationLoaderClassName(String configurationLoaderClassName) throws ClassNotFoundException {
    Class<?> configurationLoaderClass = Class.forName(configurationLoaderClassName);
    Preconditions.checkArgument(ServiceProviderConfigurationLoader.class.isAssignableFrom(configurationLoaderClass),
        "The class must implement " + ServiceProviderConfigurationLoader.class.getCanonicalName());
    this.configurationLoaderClassName = configurationLoaderClassName;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(serviceProviderBase).append(configurationLoaderClassName).hashCode();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    StaticLocationServiceConfiguration other = (StaticLocationServiceConfiguration) obj;
    return new EqualsBuilder().append(this.serviceProviderBase, other.configurationLoaderClassName).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("serviceProviderBase", serviceProviderBase)
        .append("configurationLoaderClassName", configurationLoaderClassName).toString();
  }
}
