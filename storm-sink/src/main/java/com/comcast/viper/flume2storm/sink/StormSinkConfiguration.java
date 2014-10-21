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
package com.comcast.viper.flume2storm.sink;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.MapConfiguration;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.sender.EventSenderFactory;
import com.comcast.viper.flume2storm.location.LocationServiceFactory;
import com.comcast.viper.flume2storm.location.ServiceProviderSerialization;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * Configuration for {@link StormSink}
 */
public class StormSinkConfiguration implements Supplier<Configuration> {
  /** Configuration attribute name for {@link #getBatchSize()} */
  public static final String BATCH_SIZE = "batch-size";
  /** Default value for {@value #BATCH_SIZE} */
  public static final int BATCH_SIZE_DEFAULT = 100;
  /**
   * Configuration attribute name for
   * {@link #getLocationServiceFactoryClassName()}
   */
  public static final String LOCATION_SERVICE_FACTORY_CLASS = "location.service.factory";
  /** Default value for {@value #LOCATION_SERVICE_FACTORY_CLASS} */
  public static final String LOCATION_SERVICE_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.DynamicLocationServiceFactory";
  /**
   * Configuration attribute name for
   * {@link #getServiceProviderSerializationClassName()}
   */
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS = "service.provider.serialization";
  /** Default value for {@value #SERVICE_PROVIDER_SERIALIZATION_CLASS} */
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.KryoNetServiceProviderSerialization";
  /** Configuration attribute name for {@link #getEventSenderFactoryClassName()} */
  public static final String EVENT_SENDER_FACTORY_CLASS = "event.sender.factory";
  /** Default value for {@value #EVENT_SENDER_FACTORY_CLASS} */
  public static final String EVENT_SENDER_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.connection.sender.KryoNetEventSenderFactory";
  /**
   * Configuration attribute name for
   * {@link #getConnectionParametersFactoryClassName()}
   */
  public static final String CONNECTION_PARAMETERS_FACTORY_CLASS = "connection.parameters.factory";
  /** Default value for {@value #CONNECTION_PARAMETERS_FACTORY_CLASS} */
  public static final String CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParametersFactory";

  protected int batchSize;
  protected String locationServiceFactoryClassName;
  protected String serviceProviderSerializationClassName;
  protected String eventSenderFactoryClassName;
  protected String connectionParametersFactoryClassName;
  protected Configuration configuration;

  /**
   * @param config
   *          The configuration to use
   * @return The newly built {@link StormSinkConfiguration} based on the
   *         configuration specified
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public static StormSinkConfiguration from(Configuration config) throws F2SConfigurationException {
    StormSinkConfiguration result = new StormSinkConfiguration();
    try {
      result.setBatchSize(config.getInteger(BATCH_SIZE, BATCH_SIZE_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, BATCH_SIZE, e);
    }
    try {
      result.setLocationServiceFactoryClassName(config.getString(LOCATION_SERVICE_FACTORY_CLASS,
          LOCATION_SERVICE_FACTORY_CLASS_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, LOCATION_SERVICE_FACTORY_CLASS, e);
    }
    try {
      result.setServiceProviderSerializationClassName(config.getString(SERVICE_PROVIDER_SERIALIZATION_CLASS,
          SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, SERVICE_PROVIDER_SERIALIZATION_CLASS, e);
    }
    try {
      result.setEventSenderFactoryClassName(config.getString(EVENT_SENDER_FACTORY_CLASS,
          EVENT_SENDER_FACTORY_CLASS_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, EVENT_SENDER_FACTORY_CLASS, e);
    }
    try {
      result.setConnectionParametersFactoryClassName(config.getString(CONNECTION_PARAMETERS_FACTORY_CLASS,
          CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, CONNECTION_PARAMETERS_FACTORY_CLASS, e);
    }
    result.configuration = config;
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public StormSinkConfiguration() {
    batchSize = BATCH_SIZE_DEFAULT;
    locationServiceFactoryClassName = LOCATION_SERVICE_FACTORY_CLASS_DEFAULT;
    serviceProviderSerializationClassName = SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT;
    connectionParametersFactoryClassName = CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT;
    eventSenderFactoryClassName = EVENT_SENDER_FACTORY_CLASS_DEFAULT;
    configuration = new BaseConfiguration();
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public StormSinkConfiguration(final StormSinkConfiguration other) {
    batchSize = other.batchSize;
    locationServiceFactoryClassName = other.locationServiceFactoryClassName;
    serviceProviderSerializationClassName = other.serviceProviderSerializationClassName;
    connectionParametersFactoryClassName = other.connectionParametersFactoryClassName;
    eventSenderFactoryClassName = other.eventSenderFactoryClassName;
    configuration = new MapConfiguration(ConfigurationConverter.getMap(other.configuration));
  }

  /**
   * @return The number of events per transaction
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * @param batchSize
   *          See {@link #getBatchSize()}
   */
  public void setBatchSize(int batchSize) {
    Preconditions.checkArgument(batchSize > 0, "Batch-size must be strictly positive");
    this.batchSize = batchSize;
  }

  /**
   * @return The class name of the location service factory
   */
  public String getLocationServiceFactoryClassName() {
    return locationServiceFactoryClassName;
  }

  /**
   * @param locationServiceFactoryClassName
   *          See {@link #getLocationServiceFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setLocationServiceFactoryClassName(String locationServiceFactoryClassName) throws ClassNotFoundException {
    Class<?> locationServiceFactoryClass = Class.forName(locationServiceFactoryClassName);
    Preconditions.checkArgument(LocationServiceFactory.class.isAssignableFrom(locationServiceFactoryClass),
        "The class must implement " + LocationServiceFactory.class.getCanonicalName());
    this.locationServiceFactoryClassName = locationServiceFactoryClassName;
  }

  /**
   * @return The class name of the service provider serialization
   */
  public String getServiceProviderSerializationClassName() {
    return serviceProviderSerializationClassName;
  }

  /**
   * @param serviceProviderSerializationClassName
   *          See {@link #getServiceProviderSerializationClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setServiceProviderSerializationClassName(String serviceProviderSerializationClassName)
      throws ClassNotFoundException {
    Class<?> serviceProviderSerializationClass = Class.forName(serviceProviderSerializationClassName);
    Preconditions.checkArgument(ServiceProviderSerialization.class.isAssignableFrom(serviceProviderSerializationClass),
        "The class must implement " + ServiceProviderSerialization.class.getCanonicalName());
    this.serviceProviderSerializationClassName = serviceProviderSerializationClassName;
  }

  /**
   * @return The class name of the event sender factory
   */
  public String getEventSenderFactoryClassName() {
    return eventSenderFactoryClassName;
  }

  /**
   * @param eventSenderFactoryClassName
   *          See {@link #getEventSenderFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setEventSenderFactoryClassName(String eventSenderFactoryClassName) throws ClassNotFoundException {
    Class<?> eventSenderFactoryClass = Class.forName(eventSenderFactoryClassName);
    Preconditions.checkArgument(EventSenderFactory.class.isAssignableFrom(eventSenderFactoryClass),
        "The class must implement " + EventSenderFactory.class.getCanonicalName());
    this.eventSenderFactoryClassName = eventSenderFactoryClassName;
  }

  /**
   * @return The class name of the connection parameters factory
   */
  public String getConnectionParametersFactoryClassName() {
    return connectionParametersFactoryClassName;
  }

  /**
   * @param connectionParametersFactoryClassName
   *          See {@link #getConnectionParametersFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setConnectionParametersFactoryClassName(String connectionParametersFactoryClassName)
      throws ClassNotFoundException {
    Class<?> connectionParametersFactoryClass = Class.forName(connectionParametersFactoryClassName);
    Preconditions.checkArgument(ConnectionParametersFactory.class.isAssignableFrom(connectionParametersFactoryClass),
        "The class must implement " + ConnectionParametersFactory.class.getCanonicalName());
    this.connectionParametersFactoryClassName = connectionParametersFactoryClassName;
  }

  /**
   * @see com.google.common.base.Supplier#get()
   * @return This object as a {@link Configuration}
   */
  public Configuration get() {
    return configuration;
  }

  /**
   * @param configuration
   *          The {@link FlumeSpout} configuration as a
   *          {@link StormSinkConfiguration}
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }
}
