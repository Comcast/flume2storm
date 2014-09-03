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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.comcast.viper.flume2storm.location.ServiceProviderManager;

/**
 * Unit test for {@link ServiceProviderManager}
 */
public class TestServiceProviderManager {
  /**
   * TODO better test coverage
   */
  @Test
  public void testIt() {
    SimpleServiceListener listener = new SimpleServiceListener();
    ServiceProviderManager<SimpleServiceProvider> manager = new ServiceProviderManager<SimpleServiceProvider>(listener);
    // New empty list
    List<SimpleServiceProvider> providers = new ArrayList<SimpleServiceProvider>();
    manager.set(providers);
    Assert.assertTrue(listener.getLastAdded()
        .isEmpty());
    Assert.assertTrue(listener.getLastRemoved()
        .isEmpty());

    // Adding a few
    SimpleServiceProvider ssp1 = new SimpleServiceProvider("host1", 1234);
    providers.add(ssp1);
    SimpleServiceProvider ssp2 = new SimpleServiceProvider("host2", 1234);
    providers.add(ssp2);
    SimpleServiceProvider ssp3 = new SimpleServiceProvider("host3", 1234);
    providers.add(ssp3);
    listener.clear();
    manager.set(providers);
    Assert.assertEquals(3, listener.getLastAdded()
        .size());
    Assert.assertTrue(listener.getLastAdded()
        .contains(ssp1));
    Assert.assertTrue(listener.getLastAdded()
        .contains(ssp2));
    Assert.assertTrue(listener.getLastAdded()
        .contains(ssp3));
    Assert.assertTrue(listener.getLastRemoved()
        .isEmpty());

    // Adding one, removing 2
    SimpleServiceProvider ssp4 = new SimpleServiceProvider("host4", 1234);
    providers.add(ssp4);
    providers.remove(ssp1);
    providers.remove(ssp2);
    listener.clear();
    manager.set(providers);
    Assert.assertEquals(1, listener.getLastAdded()
        .size());
    Assert.assertTrue(listener.getLastAdded()
        .contains(ssp4));
    Assert.assertEquals(2, listener.getLastRemoved()
        .size());
    Assert.assertTrue(listener.getLastRemoved()
        .contains(ssp1));
    Assert.assertTrue(listener.getLastRemoved()
        .contains(ssp2));
  }
}
