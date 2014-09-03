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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class F2SEventSerializerTest {
	private Kryo kryo;

	@Before
	public void setup() {
		kryo = new Kryo();
		KryoNetEventUtil.register(kryo);
	}

	@Test
	public void testIt() {
		final byte[] buffer = new byte[10240];
		final F2SEvent originalEvent = F2SEventFactory.getInstance().createRandomWithHeaders();
		kryo.writeClassAndObject(new Output(buffer), originalEvent);
		final Object readObject = kryo.readClassAndObject(new Input(buffer));
		Assert.assertEquals(readObject.getClass(), F2SEvent.class);
		final F2SEvent eventRead = (F2SEvent) readObject;
		Assert.assertEquals(eventRead, originalEvent);
	}
}