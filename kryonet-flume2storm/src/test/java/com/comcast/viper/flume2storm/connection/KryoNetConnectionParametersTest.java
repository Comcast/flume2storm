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
package com.comcast.viper.flume2storm.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.utility.test.TestUtils;

/**
 * Tests for {@link KryoNetConnectionParameters}
 */
public class KryoNetConnectionParametersTest {
  /**
   * Test {@link KryoNetConnectionParameters} with empty configuration
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testEmpty() throws F2SConfigurationException {
    KryoNetConnectionParameters params = KryoNetConnectionParameters.from(new BaseConfiguration());
    String localAddress;
    try {
      localAddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      localAddress = KryoNetConnectionParameters.ADDRESS_DEFAULT;
    }
    Assert.assertEquals(localAddress, params.getAddress());
    Assert.assertEquals(KryoNetConnectionParameters.PORT_DEFAULT, params.getPort());
    Assert.assertEquals(KryoNetConnectionParameters.OBJECT_BUFFER_SIZE_DEFAULT, params.getObjectBufferSize());
    Assert.assertEquals(KryoNetConnectionParameters.WRITE_BUFFER_SIZE_DEFAULT, params.getWriteBufferSize());
  }

  /**
   * Test {@link KryoNetConnectionParameters} with a full configuration
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    String hostname = RandomStringUtils.randomAlphabetic(10);
    int port = TestUtils.getRandomPositiveInt(65000);
    int objectBufferSize = TestUtils.getRandomPositiveInt(100000);
    int writeBufferSize = TestUtils.getRandomPositiveInt(100000);
    Configuration config = new BaseConfiguration();
    config.addProperty(KryoNetConnectionParameters.ADDRESS, hostname);
    config.addProperty(KryoNetConnectionParameters.PORT, port);
    config.addProperty(KryoNetConnectionParameters.OBJECT_BUFFER_SZ, objectBufferSize);
    config.addProperty(KryoNetConnectionParameters.WRITE_BUFFER_SZ, writeBufferSize);
    KryoNetConnectionParameters params = KryoNetConnectionParameters.from(config);
    Assert.assertEquals(hostname, params.getAddress());
    Assert.assertEquals(port, params.getPort());
    Assert.assertEquals(objectBufferSize, params.getObjectBufferSize());
    Assert.assertEquals(writeBufferSize, params.getWriteBufferSize());
  }

  /**
   * Test {@link KryoNetConnectionParameters} using the default constructor
   * 
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  @Test
  public void testFromScratch() throws F2SConfigurationException {
    KryoNetConnectionParameters params = new KryoNetConnectionParameters();
    String strValue = RandomStringUtils.randomAlphabetic(10);
    params.setAddress(strValue);
    Assert.assertEquals(strValue, params.getAddress());
    int value = TestUtils.getRandomPositiveInt(65000);
    params.setServerPort(value);
    Assert.assertEquals(value, params.getPort());
    value = TestUtils.getRandomPositiveInt(100000);
    params.setObjectBufferSize(value);
    Assert.assertEquals(value, params.getObjectBufferSize());
    value = TestUtils.getRandomPositiveInt(100000);
    params.setWriteBufferSize(value);
    Assert.assertEquals(value, params.getWriteBufferSize());
  }

  /**
   * Test {@link KryoNetConnectionParameters} with invalid port
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidPort1() {
    new KryoNetConnectionParameters().setServerPort(-1);
  }

  /**
   * Test {@link KryoNetConnectionParameters} with invalid port
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidPort2() {
    new KryoNetConnectionParameters().setServerPort(65536);
  }

  /**
   * Test {@link KryoNetConnectionParameters} with object buffer size
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidObjectBufferSize() {
    new KryoNetConnectionParameters().setObjectBufferSize(-1);
  }

  /**
   * Test {@link KryoNetConnectionParameters} with write buffer size
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidWriteBufferSize() {
    new KryoNetConnectionParameters().setWriteBufferSize(-1);
  }
}