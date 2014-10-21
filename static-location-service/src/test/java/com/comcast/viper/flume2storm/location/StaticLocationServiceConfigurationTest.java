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

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * Unit test for {@link StaticLocationServiceConfiguration}
 */
public class StaticLocationServiceConfigurationTest {
  /**
   * Test {@link StaticLocationServiceConfiguration#from(Configuration)}
   *
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    String configurationLoaderClassName = SimpleServiceProviderConfigurationLoader.class.getCanonicalName();
    String serviceProviderBase = "whatever";
    Configuration config = new BaseConfiguration();
    config.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS, configurationLoaderClassName);
    config.addProperty(StaticLocationServiceConfiguration.SERVICE_PROVIDER_BASE, serviceProviderBase);

    StaticLocationServiceConfiguration staticLocationServiceConfiguration = StaticLocationServiceConfiguration
        .from(config);
    Assert.assertEquals(configurationLoaderClassName,
        staticLocationServiceConfiguration.getConfigurationLoaderClassName());
    Assert.assertEquals(serviceProviderBase, staticLocationServiceConfiguration.getServiceProviderBase());
  }

  /**
   * Test invalid argument for
   * {@link StaticLocationServiceConfiguration#setConfigurationLoaderClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = ClassNotFoundException.class)
  public void testConfigurationLoaderClassName1() throws ClassNotFoundException {
    new StaticLocationServiceConfiguration().setConfigurationLoaderClassName("not-a-class");
  }

  /**
   * Test invalid argument for
   * {@link StaticLocationServiceConfiguration#setConfigurationLoaderClassName(String)}
   * 
   * @throws ClassNotFoundException
   *           If value is invalid
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConfigurationLoaderClassName2() throws ClassNotFoundException {
    new StaticLocationServiceConfiguration().setConfigurationLoaderClassName("java.lang.String");
  }

  /**
   * Test invalid argument for
   * {@link StaticLocationServiceConfiguration#setConfigurationLoaderClassName(String)}
   * 
   * @throws F2SConfigurationException
   *           If value is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  public void testConfigurationLoaderClassName3() throws F2SConfigurationException {
    Configuration config = new BaseConfiguration();
    config.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS, "not-a-class");
    StaticLocationServiceConfiguration.from(config);
  }
}
