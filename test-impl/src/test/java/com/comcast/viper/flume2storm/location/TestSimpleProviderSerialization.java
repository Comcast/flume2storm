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

import org.junit.Test;

/**
 * Unit test for {@link SimpleServiceProviderSerialization}
 */
public final class TestSimpleProviderSerialization {
  /**
   * Test serialization and deserialization
   */
  @Test
  public void testIt() {
    final String hostname = "host1";
    final int port = 1234;
    ServiceProviderSerialization<SimpleServiceProvider> serialization = new SimpleServiceProviderSerialization();
    SimpleServiceProvider ssp1 = new SimpleServiceProvider(hostname, port);
    byte[] bytes = serialization.serialize(ssp1);
    Assert.assertNotNull(bytes);
    SimpleServiceProvider ssp2 = serialization.deserialize(bytes);
    Assert.assertEquals(ssp1, ssp2);
    Assert.assertEquals(hostname, ssp2.getHostname());
    Assert.assertEquals(port, ssp2.getPort());
  }
}
