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

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptorFactory;
import com.comcast.viper.flume2storm.location.SimpleLocationServiceFactory;
import com.comcast.viper.flume2storm.location.SimpleServiceProviderSerialization;

/**
 * Unit test for {@link FlumeSpoutConfiguration}
 */
public class FlumeSpoutConfigurationTest {
  /**
   * Test {@link FlumeSpoutConfiguration#from(Configuration)}
   *
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    String locationServiceFactoryClassName = SimpleLocationServiceFactory.class.getCanonicalName();
    String serviceProviderSerializationClassName = SimpleServiceProviderSerialization.class.getCanonicalName();
    String eventReceptorFactoryClassName = SimpleEventReceptorFactory.class.getCanonicalName();
    Configuration config = new BaseConfiguration();
    config.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS, locationServiceFactoryClassName);
    config.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS,
        serviceProviderSerializationClassName);
    config.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS, eventReceptorFactoryClassName);

    FlumeSpoutConfiguration flumeSpoutConfiguration = FlumeSpoutConfiguration.from(config);
    Assert.assertEquals(locationServiceFactoryClassName, flumeSpoutConfiguration.getLocationServiceFactoryClassName());
    Assert.assertEquals(serviceProviderSerializationClassName,
        flumeSpoutConfiguration.getServiceProviderSerializationClassName());
    Assert.assertEquals(eventReceptorFactoryClassName, flumeSpoutConfiguration.getEventReceptorFactoryClassName());
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testLocationServiceFactoryClassName1() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setLocationServiceFactoryClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testLocationServiceFactoryClassName2() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setLocationServiceFactoryClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setLocationServiceFactoryClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testLocationServiceFactoryClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(FlumeSpoutConfiguration.LOCATION_SERVICE_FACTORY_CLASS, "not-a-class");
    FlumeSpoutConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testServiceProviderSerializationClassName1() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setServiceProviderSerializationClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testServiceProviderSerializationClassName2() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setServiceProviderSerializationClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setServiceProviderSerializationClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testServiceProviderSerializationClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(FlumeSpoutConfiguration.SERVICE_PROVIDER_SERIALIZATION_CLASS, "not-a-class");
    FlumeSpoutConfiguration.from(config);
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setEventReceptorFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testEventReceptorFactoryClassName1() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setEventReceptorFactoryClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setEventReceptorFactoryClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testEventReceptorFactoryClassName2() throws ClassNotFoundException {
    new FlumeSpoutConfiguration().setEventReceptorFactoryClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link FlumeSpoutConfiguration#setEventReceptorFactoryClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testEventReceptorFactoryClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(FlumeSpoutConfiguration.EVENT_RECEPTOR_FACTORY_CLASS, "not-a-class");
    FlumeSpoutConfiguration.from(config);
  }
}
