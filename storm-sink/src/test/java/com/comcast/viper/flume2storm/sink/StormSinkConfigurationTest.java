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

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParametersFactory;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSenderFactory;
import com.comcast.viper.flume2storm.location.LocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleLocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleServiceProviderSerialization;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * Unit test for {@link StormSinkConfiguration}
 */
public class StormSinkConfigurationTest {
  /**
   * Test {@link StormSinkConfiguration#from(Configuration)}
   *
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    int batchSize = 200;
    String locationServiceFactoryClassName = SimpleLocationServiceFactory.class.getCanonicalName();
    String serviceProviderSerializationClassName = SimpleServiceProviderSerialization.class.getCanonicalName();
    String eventSenderFactoryClassName = SimpleEventSenderFactory.class.getCanonicalName();
    String connectionParametersFactoryClassName = SimpleConnectionParametersFactory.class.getCanonicalName();
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.BATCH_SIZE, batchSize);
    config.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS, locationServiceFactoryClassName);
    config.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        serviceProviderSerializationClassName);
    config.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS, eventSenderFactoryClassName);
    config
        .addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS, connectionParametersFactoryClassName);

    StormSinkConfiguration stormSinkConfiguration = StormSinkConfiguration.from(config);
    Assert.assertEquals(batchSize, stormSinkConfiguration.getBatchSize());
    Assert.assertEquals(locationServiceFactoryClassName, stormSinkConfiguration.getLocationServiceFactoryClassName());
    Assert.assertEquals(serviceProviderSerializationClassName,
        stormSinkConfiguration.getServiceProviderSerializationClassName());
    Assert.assertEquals(eventSenderFactoryClassName, stormSinkConfiguration.getEventSenderFactoryClassName());
    Assert.assertEquals(connectionParametersFactoryClassName,
        stormSinkConfiguration.getConnectionParametersFactoryClassName());
  }

  /**
   * Test invalid argument for {@link StormSinkConfiguration#setBatchSize(int)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBatchSize1() throws F2SConfigurationException {
    new StormSinkConfiguration().setBatchSize(0);
  }

  /**
   * Test invalid argument for {@link StormSinkConfiguration#setBatchSize(int)}
   * using the {@link StormSinkConfiguration#from(Configuration)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testInvalidBatchSize2() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.BATCH_SIZE, "not-a-number");
    StormSinkConfiguration.from(config);
  }

  /**
   * Test invalid argument for {@link StormSinkConfiguration#setBatchSize(int)}
   * using the {@link StormSinkConfiguration#from(Configuration)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testInvalidBatchSize3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.BATCH_SIZE, "-1");
    StormSinkConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testLocationServiceFactoryClassName1() throws ClassNotFoundException {
    new StormSinkConfiguration().setLocationServiceFactoryClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testLocationServiceFactoryClassName2() throws ClassNotFoundException {
    new StormSinkConfiguration().setLocationServiceFactoryClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testLocationServiceFactoryClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.LOCATION_SERVICE_FACTORY_CLASS, "not-a-class");
    StormSinkConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testServiceProviderSerializationClassName1() throws ClassNotFoundException {
    new StormSinkConfiguration().setServiceProviderSerializationClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testServiceProviderSerializationClassName2() throws ClassNotFoundException {
    new StormSinkConfiguration().setServiceProviderSerializationClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testServiceProviderSerializationClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS, "not-a-class");
    StormSinkConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setEventSenderFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testEventSenderFactoryClassName1() throws ClassNotFoundException {
    new StormSinkConfiguration().setEventSenderFactoryClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setEventSenderFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testEventSenderFactoryClassName2() throws ClassNotFoundException {
    new StormSinkConfiguration().setEventSenderFactoryClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#setEventSenderFactoryClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testEventSenderFactoryClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.EVENT_SENDER_FACTORY_CLASS, "not-a-class");
    StormSinkConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#seConnectionParametersFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testConnectionParametersFactoryClassName1() throws ClassNotFoundException {
    new StormSinkConfiguration().setConnectionParametersFactoryClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#seConnectionParametersFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConnectionParametersFactoryClassName2() throws ClassNotFoundException {
    new StormSinkConfiguration().setConnectionParametersFactoryClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link StormSinkConfiguration#seConnectionParametersFactoryClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testConnectionParametersFactoryClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StormSinkConfiguration.CONNECTION_PARAMETERS_FACTORY_CLASS, "not-a-class");
    StormSinkConfiguration.from(config);
  }
}
