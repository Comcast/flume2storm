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
import java.util.Map.Entry;

import org.joda.time.Instant;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventBuilder;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo serializer for {@link F2SEvent}
 */
public class F2SEventSerializer extends Serializer<F2SEvent> {
  /**
   * @see com.esotericsoftware.kryo.Serializer#read(com.esotericsoftware.kryo.Kryo,
   *      com.esotericsoftware.kryo.io.Input, java.lang.Class)
   */
  @Override
  public F2SEvent read(Kryo kryo, Input input, Class<F2SEvent> arg2) {
    F2SEventBuilder builder = new F2SEventBuilder();
    builder.timestamp(new Instant(input.readLong()));
    int nbHeaders = input.readInt();
    Map<String, String> headers = new HashMap<String, String>(nbHeaders);
    for (int i = 0; i < nbHeaders; i++) {
      headers.put(input.readString(), input.readString());
    }
    builder.headers(headers);
    int bodySz = input.readInt();
    builder.body(input.readBytes(bodySz));
    return builder.get();
  }

  /**
   * @see com.esotericsoftware.kryo.Serializer#write(com.esotericsoftware.kryo.Kryo,
   *      com.esotericsoftware.kryo.io.Output, java.lang.Object)
   */
  @Override
  public void write(Kryo kryo, Output output, F2SEvent event) {
    output.writeLong(event.getTimestamp()
        .getMillis());
    output.writeInt(event.getHeaders()
        .size());
    for (Entry<String, String> header : event.getHeaders()
        .entrySet()) {
      output.writeString(header.getKey());
      output.writeString(header.getValue());
    }
    output.writeInt(event.getBody().length);
    output.writeBytes(event.getBody());
  }
}
