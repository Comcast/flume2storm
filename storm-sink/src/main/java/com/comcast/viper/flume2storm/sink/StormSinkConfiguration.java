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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.MapConfiguration;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.F2SConfigurationException.Reason;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

/**
 * Configuration for {@link StormSink}
 */
public class StormSinkConfiguration implements Supplier<Configuration> {
  public static final String BATCH_SIZE = "batch-size";
  public static final int BATCH_SIZE_DEFAULT = 100;
  public static final String TIMESTAMP_HEADER = "timestamp.header";
  public static final String TIMESTAMP_HEADER_DEFAULT = "timestamp";
  public static final String LOCATION_SERVICE_FACTORY_CLASS = "location.service.factory";
  public static final String LOCATION_SERVICE_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.DynamicLocationServiceFactory";
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS = "service.provider.serialization";
  public static final String SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT = "com.comcast.viper.flume2storm.location.KryoNetServiceProviderSerialization";
  public static final String EVENT_SENDER_FACTORY_CLASS = "event.receptor.factory";
  public static final String EVENT_SENDER_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.receptor.KryoNetEventReceptorFactory";
  public static final String CONNECTION_PARAMETERS_FACTORY_CLASS = "connection.parameters.factory";
  public static final String CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT = "com.comcast.viper.flume2storm.connection.KryoNetConnectionParametersFactory";

  protected int batchSize;
  protected String timestampHeader;
  protected String locationServiceFactoryClassName;
  protected String serviceProviderSerializationClassName;
  protected String eventSenderFactoryClassName;
  protected String connectionParametersFactoryClassName;
  protected Configuration configuration;

  /**
   * Builds a new {@link StormSinkConfiguration} based on a Configuration
   * 
   * @param config
   *          The configuration to use
   * @throws F2SConfigurationException
   *           If the class is not found in the class path
   */
  public static StormSinkConfiguration from(Configuration config) throws F2SConfigurationException {
    StormSinkConfiguration result = new StormSinkConfiguration();
    try {
      result.setBatchSize(config.getInteger(BATCH_SIZE, BATCH_SIZE_DEFAULT));
    } catch (IllegalArgumentException e) {
      // throw F2SConfigurationException.with(BATCH_SIZE, batchSize,
      // Reason.NOT_STRICLY_POSITIVE);
    } catch (Exception e) {
      // TODO finish exception handling
    }
    try {
      result.setTimestampHeader(config.getString(TIMESTAMP_HEADER, TIMESTAMP_HEADER_DEFAULT));
    } catch (IllegalArgumentException e) {
      // throw F2SConfigurationException.with(BATCH_SIZE, batchSize,
      // Reason.NOT_STRICLY_POSITIVE);
    } catch (Exception e) {
      // TODO finish exception handling
    }
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
    className = config.getString(EVENT_SENDER_FACTORY_CLASS, EVENT_SENDER_FACTORY_CLASS_DEFAULT);
    try {
      result.setEventSenderFactoryClassName(className);
    } catch (ClassNotFoundException e) {
      throw F2SConfigurationException.with(EVENT_SENDER_FACTORY_CLASS, className, e);
    }
    className = config.getString(CONNECTION_PARAMETERS_FACTORY_CLASS, CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT);
    try {
      result.setConnectionParametersFactoryClassName(className);
    } catch (ClassNotFoundException e) {
      throw F2SConfigurationException.with(CONNECTION_PARAMETERS_FACTORY_CLASS, className, e);
    }
    result.configuration = config;
    return result;
  }

  /**
   * Empty constructor - initializes with default values when available
   */
  public StormSinkConfiguration() {
    batchSize = BATCH_SIZE_DEFAULT;
    timestampHeader = TIMESTAMP_HEADER_DEFAULT;
    locationServiceFactoryClassName = LOCATION_SERVICE_FACTORY_CLASS_DEFAULT;
    serviceProviderSerializationClassName = SERVICE_PROVIDER_SERIALIZATION_CLASS_DEFAULT;
    connectionParametersFactoryClassName = CONNECTION_PARAMETERS_FACTORY_CLASS_DEFAULT;
    eventSenderFactoryClassName = EVENT_SENDER_FACTORY_CLASS_DEFAULT;
    configuration = new MapConfiguration(new HashMap<String, Object>());
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          the configuration to copy
   */
  public StormSinkConfiguration(final StormSinkConfiguration other) {
    batchSize = other.batchSize;
    timestampHeader = other.timestampHeader;
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
   * @throws F2SConfigurationException
   *           If the batch size is not stricly positive
   */
  public void setBatchSize(int batchSize) throws F2SConfigurationException {
    Preconditions.checkArgument(batchSize > 0);
    this.batchSize = batchSize;
  }

  /**
   * @return The name of the header that contains the timestamp of the event
   */
  public String getTimestampHeader() {
    return timestampHeader;
  }

  /**
   * @param timestampHeader
   *          See {@link #getTimestampHeader()}
   */
  public void setTimestampHeader(String timestampHeader) {
    if (timestampHeader.length() == 0)

      this.timestampHeader = timestampHeader;
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
   * @return The class name of the event sender factory
   */
  public String getEventSenderFactoryClassName() {
    return eventSenderFactoryClassName;
  }

  /**
   * @param eventSenderFactory
   *          See {@link #getEventSenderFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setEventSenderFactoryClassName(String eventSenderFactory) throws ClassNotFoundException {
    Class.forName(eventSenderFactory);
    this.eventSenderFactoryClassName = eventSenderFactory;
  }

  /**
   * @return The class name of the connection parameters factory
   */
  public String getConnectionParametersFactoryClassName() {
    return connectionParametersFactoryClassName;
  }

  /**
   * @param eventSenderFactory
   *          See {@link #getConnectionParametersFactoryClassName()}
   * @throws ClassNotFoundException
   *           If the class specified is not found in the classpath
   */
  public void setConnectionParametersFactoryClassName(String eventSenderFactory) throws ClassNotFoundException {
    Class.forName(eventSenderFactory);
    this.connectionParametersFactoryClassName = eventSenderFactory;
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
