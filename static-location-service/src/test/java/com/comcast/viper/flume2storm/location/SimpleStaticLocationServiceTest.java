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

import static com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters.HOSTNAME;
import static com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters.PORT;
import static com.comcast.viper.flume2storm.location.StaticLocationServiceConfiguration.SERVICE_PROVIDER_BASE;
import static com.comcast.viper.flume2storm.location.StaticLocationServiceConfiguration.SERVICE_PROVIDER_BASE_DEFAULT;
import static com.comcast.viper.flume2storm.location.StaticLocationServiceConfiguration.SERVICE_PROVIDER_LIST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;

/**
 * Test the {@link StaticLocationService}
 */
public class SimpleStaticLocationServiceTest {
  private static final String SP1 = "quick";
  private static final SimpleConnectionParameters CP1 = new SimpleConnectionParameters("machine1.mydomain.com", 1000);
  private static final String SP2 = "brown";
  private static final SimpleConnectionParameters CP2 = new SimpleConnectionParameters("localhost", 1000);
  private static final String SP3 = "fox";
  private static final SimpleConnectionParameters CP3 = new SimpleConnectionParameters("machine2.mydomain.com", 1001);
  private static final String SIMPLE_SERVICE_PROVIDER_BASE = "simple.service.providers";

  private Configuration configuration;

  private static String buildPName(String... elements) {
    return StringUtils.join(elements, ".");
  }

  /**
   * Loads the {@link StaticLocationService} configuration file
   */
  @Before
  public void init() {
    configuration = new BaseConfiguration();
    // List of service providers to load
    configuration.addProperty(SERVICE_PROVIDER_LIST, StringUtils.join(new String[] { SP1, SP2 }, " "));

    // First service provider
    configuration.addProperty(buildPName(SERVICE_PROVIDER_BASE_DEFAULT, SP1, HOSTNAME), CP1.getHostname());
    configuration.addProperty(buildPName(SERVICE_PROVIDER_BASE_DEFAULT, SP1, PORT), CP1.getPort());

    // Second service provider
    configuration.addProperty(buildPName(SERVICE_PROVIDER_BASE_DEFAULT, SP2, HOSTNAME), CP2.getHostname());
    configuration.addProperty(buildPName(SERVICE_PROVIDER_BASE_DEFAULT, SP2, PORT), CP2.getPort());

    // Third service provider
    configuration.addProperty(buildPName(SIMPLE_SERVICE_PROVIDER_BASE, SP3, HOSTNAME), CP3.getHostname());
    configuration.addProperty(buildPName(SIMPLE_SERVICE_PROVIDER_BASE, SP3, PORT), CP3.getPort());

    configuration.addProperty("some.other.variable", "whatever");
  }

  /**
   * Test a configuration missing the {@link ServiceProviderConfigurationLoader}
   * attribute
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test(expected = F2SConfigurationException.class)
  @SuppressWarnings("unused")
  public void testInvalidConfiguration() throws F2SConfigurationException {
    new SimpleStaticLocationService(configuration);
  }

  /**
   * Test a configuration using the default
   * {@link StaticLocationServiceConfiguration#SERVICE_PROVIDER_BASE}
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testBaseConfiguration() throws F2SConfigurationException {
    // Adding Service provider loader
    configuration.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS,
        SimpleServiceProviderConfigurationLoader.class.getName());

    // Loading static location service
    SimpleStaticLocationService ssLocationService = new SimpleStaticLocationService(configuration);

    Collection<SimpleServiceProvider> serviceProviders = ssLocationService.getServiceProviders();
    assertThat(serviceProviders).hasSize(2);
    assertThat(serviceProviders).contains(new SimpleServiceProvider(CP1), new SimpleServiceProvider(CP2));
  }

  /**
   * Test a configuration using a specific
   * {@link StaticLocationServiceConfiguration#SERVICE_PROVIDER_BASE}
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testWithSimpleBase() throws F2SConfigurationException {
    // Adding Service provider loader
    configuration.addProperty(StaticLocationServiceConfiguration.CONFIGURATION_LOADER_CLASS,
        SimpleServiceProviderConfigurationLoader.class.getName());
    // Changing service provider base
    configuration.addProperty(SERVICE_PROVIDER_BASE, SIMPLE_SERVICE_PROVIDER_BASE);
    // Changing list of service providers
    configuration.clearProperty(SERVICE_PROVIDER_LIST);
    configuration.addProperty(SERVICE_PROVIDER_LIST, SP3);

    // Loading static location service
    SimpleStaticLocationService ssLocationService = new SimpleStaticLocationService(configuration);

    Collection<SimpleServiceProvider> serviceProviders = ssLocationService.getServiceProviders();
    assertThat(serviceProviders).hasSize(1);
    assertThat(serviceProviders).contains(new SimpleServiceProvider(CP3));
  }
}
