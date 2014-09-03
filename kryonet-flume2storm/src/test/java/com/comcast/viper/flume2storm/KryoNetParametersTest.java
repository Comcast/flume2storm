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
package com.comcast.viper.flume2storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class KryoNetParametersTest {
	private static Random random = new Random();

	/**
	 * @param n
	 *            Max int allowed
	 * @return A random number between 1 and n
	 */
	private static final int getRandomInt(int n) {
		Preconditions.checkArgument(n > 1,
				"Cannot generate this kind of number!");
		return random.nextInt(n - 1) + 1;
	}

	@Test
	public void testEmpty() throws Exception {
		KryoNetParameters params = KryoNetParameters.from(new MapConfiguration(
				new HashMap<String, String>()));
		Assert.assertEquals(KryoNetParameters.CONNECTION_TIMEOUT_DEFAULT,
				params.getConnectionTimeout());
		Assert.assertEquals(KryoNetParameters.RETRY_SLEEP_DELAY_DEFAULT,
				params.getRetrySleepDelay());
		Assert.assertEquals(KryoNetParameters.RECONNECTION_DELAY_DEFAULT,
				params.getReconnectionDelay());
		Assert.assertEquals(KryoNetParameters.TERMINATION_TO_DEFAULT,
				params.getTerminationTimeout());
	}

	@Test
	public void testFromConfiguration() throws Exception {
		int connectionTo = getRandomInt(100000);
		int retrySleepDelay = getRandomInt(100000);
		int reconnectionDelay = getRandomInt(100000);
		int terminationTo = getRandomInt(100000);
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(KryoNetParameters.CONNECTION_TIMEOUT, connectionTo);
		map.put(KryoNetParameters.RETRY_SLEEP_DELAY, retrySleepDelay);
		map.put(KryoNetParameters.RECONNECTION_DELAY,
				reconnectionDelay);
		map.put(KryoNetParameters.TERMINATION_TO, terminationTo);
		KryoNetParameters params = KryoNetParameters.from(new MapConfiguration(
				map));
		Assert.assertEquals(connectionTo, params.getConnectionTimeout());
		Assert.assertEquals(retrySleepDelay, params.getRetrySleepDelay());
		Assert.assertEquals(reconnectionDelay, params.getReconnectionDelay());
		Assert.assertEquals(terminationTo, params.getTerminationTimeout());
	}

	@Test
	public void testFromScratch() throws Exception {
		KryoNetParameters params = new KryoNetParameters();
		int value = getRandomInt(100000);
		params.setConnectionTimeout(value);
		Assert.assertEquals(value, params.getConnectionTimeout());
		value = getRandomInt(100000);
		params.setRetrySleepDelay(value);
		Assert.assertEquals(value, params.getRetrySleepDelay());
		value = getRandomInt(100000);
		params.setReconnectionDelay(value);
		Assert.assertEquals(value, params.getReconnectionDelay());
		value = getRandomInt(100000);
		params.setTerminationTimeout(value);
		Assert.assertEquals(value, params.getTerminationTimeout());
	}
}