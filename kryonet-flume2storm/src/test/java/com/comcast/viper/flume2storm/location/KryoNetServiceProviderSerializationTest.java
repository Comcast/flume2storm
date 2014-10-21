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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;

public class KryoNetServiceProviderSerializationTest {
	@Test
	public void testOne() {
		KryoNetServiceProvider knSP = new KryoNetServiceProvider(new KryoNetConnectionParameters());
		KryoNetServiceProviderSerialization knSPSerialization = new KryoNetServiceProviderSerialization();
		byte[] bytes = knSPSerialization.serialize(knSP);
		Assert.assertEquals(knSP, knSPSerialization.deserialize(bytes));
	}

	@Test
	public void testLong() {
		KryoNetConnectionParameters knCP = new KryoNetConnectionParameters();
		// Way past:
		// "According to RFC 1035 the length of a FQDN is limited to 255 characters"
		knCP.setAddress(RandomStringUtils.randomAlphanumeric(24000));
		knCP.setServerPort(8000);
		knCP.setObjectBufferSize(1024);
		knCP.setWriteBufferSize(10240);
		KryoNetServiceProvider knSP = new KryoNetServiceProvider(knCP);
		KryoNetServiceProviderSerialization knSPSerialization = new KryoNetServiceProviderSerialization();
		byte[] bytes = knSPSerialization.serialize(knSP);
		Assert.assertEquals(knSP, knSPSerialization.deserialize(bytes));
	}
}