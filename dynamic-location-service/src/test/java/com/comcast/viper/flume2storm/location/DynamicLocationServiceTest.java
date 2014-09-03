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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.comcast.viper.flume2storm.utility.test.TestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkClientTestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkServerTestUtils;
import com.comcast.viper.flume2storm.zookeeper.ZkTestUtils;

/**
 * Unit test for Dynamic Location Service
 */
public class DynamicLocationServiceTest extends ZkClientTestUtils {
  private DynamicLocationServiceConfiguration config;

  @Before
  public void init() throws Exception {
    config = new DynamicLocationServiceConfiguration();
    config.setConnectionStr(ZkTestUtils.HOST + ":" + ZkServerTestUtils.PORT);
    config.setConnectionTimeout(ZkTestUtils.TEST_TIMEOUT);
    config.setBasePath("LocationService");
    config.setServiceName("TestDynamic");

    ZkServerTestUtils.startZkServer();
    ZkServerTestUtils.waitZkServerOn();
  }

  @After
  public void terminate() throws Exception {
    ZkServerTestUtils.stopZkServer();
    System.out.println("Server stopped.");
    ZkServerTestUtils.waitZkServerOff();
    System.out.println("All done.");
  }

  private static class FromHost {
    private DynamicLocationService<SimpleServiceProvider> locationService;
    private List<SimpleServiceProvider> recentlyAddedProviders = new ArrayList<SimpleServiceProvider>(10);
    private List<SimpleServiceProvider> recentlyRemovedProviders = new ArrayList<SimpleServiceProvider>(10);
    private AtomicBoolean notified = new AtomicBoolean(false);

    public FromHost(DynamicLocationServiceConfiguration config) {
      locationService = new DynamicLocationService<SimpleServiceProvider>(config,
          new SimpleServiceProviderSerialization());
      locationService.addListener(new ServiceListener<SimpleServiceProvider>() {
        @Override
        public void onProviderAdded(SimpleServiceProvider serviceProvider) {
          System.out.println("Received sp notification: added " + serviceProvider);
          recentlyAddedProviders.add(serviceProvider);
          notified.set(true);
        }

        @Override
        public void onProviderRemoved(SimpleServiceProvider serviceProvider) {
          System.out.println("Received sp notification: removed " + serviceProvider);
          recentlyRemovedProviders.add(serviceProvider);
          notified.set(true);
        }
      });
    }

    public void waitForNotification(int timeout) throws InterruptedException {
      TestUtils.waitFor(new TestCondition() {
        @Override
        public boolean evaluate() {
          return notified.get();
        }
      }, timeout);
    }

    public void connect() throws InterruptedException {
      if (!locationService.start())
        Assert.fail("Failed to start the location service");
    }

    public void disconnect() throws InterruptedException {
      if (!locationService.stop()) {
        Assert.fail("Failed to stop the location service");
      }
      TestUtils.waitFor(new TestCondition() {
        @Override
        public boolean evaluate() {
          return !locationService.isConnected();
        }
      }, TEST_TIMEOUT);
    }

    public void resetNotifications() {
      recentlyAddedProviders.clear();
      recentlyRemovedProviders.clear();
      notified.set(false);
    }
  }

  @Test
  public void testIt() throws Exception {
    FromHost fromHost1 = new FromHost(config);
    fromHost1.connect();
    Assert.assertEquals(0, fromHost1.locationService.getServiceProviders().size());

    FromHost fromHost2 = new FromHost(config);
    fromHost2.connect();
    Assert.assertEquals(0, fromHost2.locationService.getServiceProviders().size());

    // Registering a ServiceProvider from host1
    SimpleServiceProvider sp1 = new SimpleServiceProvider("myserver1.mydomain.com", 1234);
    fromHost1.locationService.register(sp1);
    fromHost2.waitForNotification(TEST_TIMEOUT);
    assertThat(fromHost1.recentlyAddedProviders).hasSize(1).contains(sp1);
    assertThat(fromHost1.recentlyRemovedProviders).isEmpty();
    assertThat(fromHost1.locationService.getServiceProviders()).hasSize(1).contains(sp1);
    fromHost1.resetNotifications();

    assertThat(fromHost2.recentlyAddedProviders).hasSize(1).contains(sp1);
    assertThat(fromHost2.recentlyRemovedProviders).isEmpty();
    assertThat(fromHost2.locationService.getServiceProviders()).hasSize(1).contains(sp1);
    fromHost2.resetNotifications();

    // Registering another ServiceProvider from host1
    SimpleServiceProvider sp2 = new SimpleServiceProvider("myserver2.mydomain.com", 4567);
    fromHost2.locationService.register(sp2);
    fromHost1.waitForNotification(TEST_TIMEOUT);
    assertThat(fromHost1.recentlyAddedProviders).hasSize(1).contains(sp2);
    assertThat(fromHost1.recentlyRemovedProviders).isEmpty();
    assertThat(fromHost1.locationService.getServiceProviders()).hasSize(2).contains(sp1, sp2);
    fromHost1.resetNotifications();

    assertThat(fromHost2.recentlyAddedProviders).hasSize(1).contains(sp2);
    assertThat(fromHost2.recentlyRemovedProviders).isEmpty();
    assertThat(fromHost2.locationService.getServiceProviders()).hasSize(2).contains(sp1, sp2);
    fromHost2.resetNotifications();

    // Unregistering first ServiceProvider
    fromHost1.locationService.unregister(sp1);
    fromHost2.waitForNotification(TEST_TIMEOUT);
    assertThat(fromHost1.recentlyAddedProviders).isEmpty();
    assertThat(fromHost1.recentlyRemovedProviders).hasSize(1).contains(sp1);
    assertThat(fromHost1.locationService.getServiceProviders()).hasSize(1).contains(sp2);
    fromHost1.resetNotifications();

    assertThat(fromHost2.recentlyAddedProviders).isEmpty();
    assertThat(fromHost2.recentlyRemovedProviders).hasSize(1).contains(sp1);
    assertThat(fromHost2.locationService.getServiceProviders()).hasSize(1).contains(sp2);
    fromHost2.resetNotifications();

    fromHost1.disconnect();
    fromHost2.disconnect();
    System.out.println("Test done.");
  }
}
