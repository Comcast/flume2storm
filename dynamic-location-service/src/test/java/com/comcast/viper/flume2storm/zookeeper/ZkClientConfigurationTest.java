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
package com.comcast.viper.flume2storm.zookeeper;

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;

public class ZkClientConfigurationTest {
  @Test
  public void testFromConfiguration() throws F2SConfigurationException {
    String connectionStr = "host1.whatever.org";
    int sessionTimeout = 1111;
    int connectionTimeout = 2222;
    int reconnectionDelay = 3333;
    int terminationTimeout = 4444;
    Configuration config = new BaseConfiguration();
    config.addProperty(ZkClientConfiguration.CONNECTION_STRING, connectionStr);
    config.addProperty(ZkClientConfiguration.SESSION_TIMEOUT, sessionTimeout);
    config.addProperty(ZkClientConfiguration.CONNECTION_TIMEOUT, connectionTimeout);
    config.addProperty(ZkClientConfiguration.RECONNECTION_DELAY, reconnectionDelay);
    config.addProperty(ZkClientConfiguration.TERMINATION_TIMEOUT, terminationTimeout);

    ZkClientConfiguration zkClientConfiguration = ZkClientConfiguration.from(config);
    Assert.assertEquals(connectionStr, zkClientConfiguration.getConnectionStr());
    Assert.assertEquals(sessionTimeout, zkClientConfiguration.getSessionTimeout());
    Assert.assertEquals(connectionTimeout, zkClientConfiguration.getConnectionTimeout());
    Assert.assertEquals(reconnectionDelay, zkClientConfiguration.getReconnectionDelay());
    Assert.assertEquals(terminationTimeout, zkClientConfiguration.getTerminationTimeout());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConnectionStr() {
    new ZkClientConfiguration().setConnectionStr(StringUtils.EMPTY);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSessionTimeout() {
    new ZkClientConfiguration().setSessionTimeout(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConnectionTimeout() {
    new ZkClientConfiguration().setConnectionTimeout(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidReconnectionDelay() {
    new ZkClientConfiguration().setReconnectionDelay(-10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTerminationTimeout() {
    new ZkClientConfiguration().setTerminationTimeout(0);
  }
}
