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
package com.comcast.viper.flume2storm.spout;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.google.common.base.Supplier;

/**
 * Configuration for {@link FlumeSpout}
 */
public class FlumeSpoutConfiguration implements Supplier<Configuration>, Serializable {
  private static final long serialVersionUID = -8862782920258510684L;
  /**
   * Configuration attribute name for
   * {@link #getLocationServiceFactoryClassName()}
   */
  public static final String LOCATION_SERVICE_FACTORY_CLASS = "location.service.factory";
  /**
   * Default value for {@value #LOCATION_SERVICE_FACTORY_CLASS}
   */
  public static final String LOCATION_SERVICE_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.DynamicLocationServiceFactory";
  /**
   * Configuration attribute name for
   * {@link #getServiceProviderSerializationClassName()}
   */
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS = "service.provider.serialization";
  /**
   * Default value for {@value #SERVICE_PROVIDER_SERIALIZATION_CLASS}
   */
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.KryoNetServiceProviderSerialization";
  /**
   * Configuration attribute name for
   * {@link #getEventReceptorFactoryClassName()}
   */
  public static final String EVENT_RECEPTOR_FACTORY_CLASS = "event.receptor.factory";
  /**
   * Default value for {@value #EVENT_RECEPTOR_FACTORY_CLASS}
   */
  public static final String EVENT_RECEPTOR_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.receptor.KryoNetEventReceptorFactory";

  protected String locationServiceFactoryClassName;
  protected String serviceProviderSerializationClassName;
  protected String eventReceptorFactoryClassName;
  protected Map<String, Object> configuration;

  /**
   * Builds a new {@link FlumeSpoutConfiguration} based on a Configuration
   * 
   * @param config
   *          The configuration to use
   * @return The newly created {@link F2SConfiguration}
   * @throws F2SConfigurationException
   *           If the class is not found in the class path
   */
  public static FlumeSpoutConfiguration from(Configuration config) throws F2SConfigurationException {
    FlumeSpoutConfiguration result = new FlumeSpoutConfiguration();
    String className = config.getString(LOCATION_SERVICE_FACTORY_CLASS, LOCATION_SERVICE_FACTORY_CLASS_DEFAULT);
    try {
      result.setLocationServiceFactoryClassName(className);
    } catch (ClassNotFoundException e) {
      throw F2SConfigurationException.with(LOCATION_SERVICE_FACTORY_CLASS, className, e);
    }
    className = config.getString(SERVICE_PROVIDER_SERIALIZATION_CLASS, SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT);
    try {
      result.setServiceProviderSerializationClassName(className);
    } catch (ClassNotFoundException e) {
      throw F2SConfigurationException.with(SERVICE_PROVIDER_SERIALIZATION_CLASS, className, e);
    }
    className = config.getString(EVENT_RECEPTOR_FACTORY_CLASS, EVENT_RECEPTOR_FACTORY_CLASS_DEFAULT);
    try {
      result.setEventReceptorFactoryClassName(className);
    } catch (ClassNotFoundException e) {
      throw F2SConfigurationException.with(EVENT_RECEPTOR_FACTORY_CLASS, className, e);
    }
    result.configuration = getMapFromConfiguration(config);
    return result;
  }

  /**
   * @param configuration
   *          A configuration object
   * @return A map that contains the configuration key/values (in order to
   *         serialize it via Kryo)
   */
  protected static Map<String, Object> getMapFromConfiguration(Configuration configuration) {
    Map<String, Object> result = new HashMap<String, Object>();
    Iterator<?> it = configuration.getKeys();
    while (it.hasNext()) {
      String key = it.next().toString();
      result.put(key, configuration.getProperty(key));
    }
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public FlumeSpoutConfiguration() {
    locationServiceFactoryClassName = LOCATION_SERVICE_FACTORY_CLASS_DEFAULT;
    serviceProviderSerializationClassName = SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT;
    eventReceptorFactoryClassName = EVENT_RECEPTOR_FACTORY_CLASS_DEFAULT;
    configuration = new HashMap<String, Object>();
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public FlumeSpoutConfiguration(final FlumeSpoutConfiguration other) {
    locationServiceFactoryClassName = other.locationServiceFactoryClassName;
    serviceProviderSerializationClassName = other.serviceProviderSerializationClassName;
    eventReceptorFactoryClassName = other.eventReceptorFactoryClassName;
    configuration = new HashMap<String, Object>(other.configuration);
  }

  /**
   * @return The class name of the location service factory
   */
  public String getLocationServiceFactoryClassName() {
    return locationServiceFactoryClassName;
  }

  /**
   * @param locationServiceFactoryClass
   *          See {@link #getLocationServiceFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setLocationServiceFactoryClassName(String locationServiceFactoryClass) throws ClassNotFoundException {
    Class.forName(locationServiceFactoryClass);
    this.locationServiceFactoryClassName = locationServiceFactoryClass;
  }

  /**
   * @return The class name of the service provider serialization
   */
  public String getServiceProviderSerializationClassName() {
    return serviceProviderSerializationClassName;
  }

  /**
   * @param serviceProviderSerializationClass
   *          See {@link #getServiceProviderSerializationClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setServiceProviderSerializationClassName(String serviceProviderSerializationClass)
      throws ClassNotFoundException {
    Class.forName(serviceProviderSerializationClass);
    this.serviceProviderSerializationClassName = serviceProviderSerializationClass;
  }

  /**
   * @return The class name of the event receptor factory
   */
  public String getEventReceptorFactoryClassName() {
    return eventReceptorFactoryClassName;
  }

  /**
   * @param eventReceptorFactory
   *          See {@link #getEventReceptorFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setEventReceptorFactoryClassName(String eventReceptorFactory) throws ClassNotFoundException {
    Class.forName(serviceProviderSerializationClassName);
    this.eventReceptorFactoryClassName = eventReceptorFactory;
  }

  /**
   * @see com.google.common.base.Supplier#get()
   * @return This object as a {@link Configuration}
   */
  public Configuration get() {
    return new MapConfiguration(configuration);
  }

  /**
   * @param map
   *          The {@link FlumeSpout} configuration as a {@link Map}
   */
  public void setConfiguration(Map<String, Object> map) {
    this.configuration = map;
  }

  /**
   * @param configuration
   *          The {@link FlumeSpout} configuration as a
   *          {@link FlumeSpoutConfiguration}
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = getMapFromConfiguration(configuration);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(locationServiceFactoryClassName).append(serviceProviderSerializationClassName)
        .append(eventReceptorFactoryClassName).append(configuration).hashCode();
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
    FlumeSpoutConfiguration other = (FlumeSpoutConfiguration) obj;
    return new EqualsBuilder().append(this.eventReceptorFactoryClassName, other.eventReceptorFactoryClassName)
        .append(this.locationServiceFactoryClassName, other.locationServiceFactoryClassName)
        .append(this.serviceProviderSerializationClassName, other.serviceProviderSerializationClassName)
        .append(this.configuration, other.configuration).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder("FlumeSpoutConfiguration")
        .append("locationServiceFactoryClass", locationServiceFactoryClassName)
        .append("serviceProviderSerializationClass", serviceProviderSerializationClassName)
        .append("eventReceptorFactory", eventReceptorFactoryClassName).append("configuration", configuration)
        .toString();
  }
}
