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

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class SimpleStaticLocationServiceTest {
	@Test
	public void testIt() throws ConfigurationException {
		SimpleStaticLocationService.createInstance(new PropertiesConfiguration("src/test/resources/simpleServiceProviders.properties"));
		Collection<SimpleServiceProvider> serviceProviders = SimpleStaticLocationService.getInstance().getServiceProviders();
		Assert.assertEquals(3, serviceProviders.size());
		SortedSet<SimpleServiceProvider> orderedServiceProviders = new TreeSet<SimpleServiceProvider>();
		orderedServiceProviders.addAll(serviceProviders);
		SimpleServiceProvider firstSP = orderedServiceProviders.first();
		Assert.assertEquals("localhost", firstSP.getHostname());
		Assert.assertEquals(1000, firstSP.getPort());
		SimpleServiceProvider lastSP = orderedServiceProviders.last();
		Assert.assertEquals("machine2.mydomain.com", lastSP.getHostname());
		Assert.assertEquals(1001, lastSP.getPort());
	}
}
