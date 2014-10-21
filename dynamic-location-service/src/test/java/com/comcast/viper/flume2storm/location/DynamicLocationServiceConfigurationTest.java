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
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * Unit test for Dynamic Location Service Configuration
 */
public class DynamicLocationServiceConfigurationTest {
  /**
   * Test building {@link DynamicLocationServiceConfiguration} from a
   * {@link Configuration} object
   * 
   * @throws F2SConfigurationException
   */
  @Ignore
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    String connectionStr = "host1.whatever.org";
    int sessionTimeout = 1111;
    int connectionTimeout = 2222;
    int reconnectionDelay = 3333;
    int terminationTimeout = 4444;
    String basePath = "/whatever";
    String serviceName = "yo";
    Configuration config = new BaseConfiguration();
    config.addProperty(DynamicLocationServiceConfiguration.CONNECTION_STRING, connectionStr);
    config.addProperty(DynamicLocationServiceConfiguration.SESSION_TIMEOUT, sessionTimeout);
    config.addProperty(DynamicLocationServiceConfiguration.CONNECTION_TIMEOUT, connectionTimeout);
    config.addProperty(DynamicLocationServiceConfiguration.RECONNECTION_DELAY, reconnectionDelay);
    config.addProperty(DynamicLocationServiceConfiguration.TERMINATION_TIMEOUT, terminationTimeout);
    config.addProperty(DynamicLocationServiceConfiguration.BASE_PATH, basePath);
    config.addProperty(DynamicLocationServiceConfiguration.TERMINATION_TIMEOUT, serviceName);

    DynamicLocationServiceConfiguration dlsConfiguration = DynamicLocationServiceConfiguration.from(config);
    Assert.assertEquals(connectionStr, dlsConfiguration.getConnectionStr());
    Assert.assertEquals(sessionTimeout, dlsConfiguration.getSessionTimeout());
    Assert.assertEquals(connectionTimeout, dlsConfiguration.getConnectionTimeout());
    Assert.assertEquals(reconnectionDelay, dlsConfiguration.getReconnectionDelay());
    Assert.assertEquals(terminationTimeout, dlsConfiguration.getTerminationTimeout());
    Assert.assertEquals(basePath, dlsConfiguration.getBasePath());
    Assert.assertEquals(serviceName, dlsConfiguration.getServiceName());
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConnectionStr() {
    new DynamicLocationServiceConfiguration().setConnectionStr(StringUtils.EMPTY);
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSessionTimeout() {
    new DynamicLocationServiceConfiguration().setSessionTimeout(-1);
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConnectionTimeout() {
    new DynamicLocationServiceConfiguration().setConnectionTimeout(0);
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidReconnectionDelay() {
    new DynamicLocationServiceConfiguration().setReconnectionDelay(-10);
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTerminationTimeout() {
    new DynamicLocationServiceConfiguration().setTerminationTimeout(0);
  }

  /**
   * Test invalid argument
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidServiceName() {
    new DynamicLocationServiceConfiguration().setServiceName(StringUtils.EMPTY);
  }
}
