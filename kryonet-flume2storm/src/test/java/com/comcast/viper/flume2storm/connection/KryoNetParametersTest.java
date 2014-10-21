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

import junit.framework.Assert;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import com.comcast.viper.flume2storm.utility.test.TestUtils;

public class KryoNetParametersTest {
  @Test
  public void testEmpty() throws Exception {
    KryoNetParameters params = KryoNetParameters.from(new BaseConfiguration());
    Assert.assertEquals(KryoNetParameters.CONNECTION_TIMEOUT_DEFAULT, params.getConnectionTimeout());
    Assert.assertEquals(KryoNetParameters.RETRY_SLEEP_DELAY_DEFAULT, params.getRetrySleepDelay());
    Assert.assertEquals(KryoNetParameters.RECONNECTION_DELAY_DEFAULT, params.getReconnectionDelay());
    Assert.assertEquals(KryoNetParameters.TERMINATION_TO_DEFAULT, params.getTerminationTimeout());
  }

  @Test
  public void testFromConfiguration() throws Exception {
    int connectionTo = TestUtils.getRandomPositiveInt(100000);
    int retrySleepDelay = TestUtils.getRandomPositiveInt(100000);
    int reconnectionDelay = TestUtils.getRandomPositiveInt(100000);
    int terminationTo = TestUtils.getRandomPositiveInt(100000);
    Configuration config = new BaseConfiguration();
    config.addProperty(KryoNetParameters.CONNECTION_TIMEOUT, connectionTo);
    config.addProperty(KryoNetParameters.RETRY_SLEEP_DELAY, retrySleepDelay);
    config.addProperty(KryoNetParameters.RECONNECTION_DELAY, reconnectionDelay);
    config.addProperty(KryoNetParameters.TERMINATION_TO, terminationTo);
    KryoNetParameters params = KryoNetParameters.from(config);
    Assert.assertEquals(connectionTo, params.getConnectionTimeout());
    Assert.assertEquals(retrySleepDelay, params.getRetrySleepDelay());
    Assert.assertEquals(reconnectionDelay, params.getReconnectionDelay());
    Assert.assertEquals(terminationTo, params.getTerminationTimeout());
  }

  @Test
  public void testFromScratch() throws Exception {
    KryoNetParameters params = new KryoNetParameters();
    int value = TestUtils.getRandomPositiveInt(100000);
    params.setConnectionTimeout(value);
    Assert.assertEquals(value, params.getConnectionTimeout());
    value = TestUtils.getRandomPositiveInt(100000);
    params.setRetrySleepDelay(value);
    Assert.assertEquals(value, params.getRetrySleepDelay());
    value = TestUtils.getRandomPositiveInt(100000);
    params.setReconnectionDelay(value);
    Assert.assertEquals(value, params.getReconnectionDelay());
    value = TestUtils.getRandomPositiveInt(100000);
    params.setTerminationTimeout(value);
    Assert.assertEquals(value, params.getTerminationTimeout());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidConnectionTimeout() {
    new KryoNetParameters().setConnectionTimeout(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidRetrySleepDelay() {
    new KryoNetParameters().setRetrySleepDelay(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidReconnectionDelay() {
    new KryoNetParameters().setReconnectionDelay(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTerminationTimeout() {
    new KryoNetParameters().setTerminationTimeout(-1);
  }
}