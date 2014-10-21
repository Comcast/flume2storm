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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.comcast.viper.flume2storm.F2SConfigurationException;

/**
 * Unit test for {@link SimpleLocationService}
 */
public class TestSimpleLocationService {
  /**
   * Test {@link SimpleLocationService}s
   * 
   * @throws F2SConfigurationException
   *           Never actually thrown
   */
  @Test
  public void testWithListeners() throws F2SConfigurationException {
    // Creating location service
    SimpleLocationServiceFactory locationServiceFactory = new SimpleLocationServiceFactory();
    // factory parameters actually unused
    SimpleLocationService locationService = locationServiceFactory.create(null, null);
    locationService.start();

    // Adding listener
    SimpleServiceListener listener = new SimpleServiceListener();
    locationService.addListener(listener);

    // Adding a service provider
    SimpleServiceProvider ssp1 = new SimpleServiceProvider("host1", 1234);
    locationService.register(ssp1);
    assertThat(listener.getLastAdded()).hasSize(1).contains(ssp1);
    listener.clear();
    assertThat(locationService.getServiceProviders()).hasSize(1).contains(ssp1);

    // Adding another service provider
    SimpleServiceProvider ssp2 = new SimpleServiceProvider("host2", 1234);
    locationService.register(ssp2);
    assertThat(listener.getLastAdded()).hasSize(1).contains(ssp2);
    listener.clear();
    assertThat(locationService.getServiceProviders()).hasSize(2).contains(ssp1, ssp2);

    // Removing first service provider
    locationService.unregister(ssp1);
    assertThat(listener.getLastRemoved()).hasSize(1).contains(ssp1);
    listener.clear();
    assertThat(locationService.getServiceProviders()).hasSize(1).contains(ssp2);

    // Removing second service provider
    locationService.unregister(ssp2);
    assertThat(listener.getLastRemoved()).hasSize(1).contains(ssp2);
    listener.clear();
    assertThat(locationService.getServiceProviders()).isEmpty();

    // Removing listener and adding first service provider (no notification)
    locationService.removeListener(listener);
    locationService.register(ssp1);
    assertThat(listener.getLastAdded()).isEmpty();

    // Re-adding listener
    locationService.addListener(listener);
    assertThat(listener.getLastAdded()).hasSize(1).contains(ssp1);
    listener.clear();
    assertThat(locationService.getServiceProviders()).hasSize(1).contains(ssp1);

    locationService.stop();
  }
}
