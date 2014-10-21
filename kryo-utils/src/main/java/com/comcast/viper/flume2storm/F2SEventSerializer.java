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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventBuilder;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

/**
 * Kryo serializer for {@link F2SEvent}.<br />
 */
public class F2SEventSerializer extends Serializer<F2SEvent> {
  protected static final Logger LOG = LoggerFactory.getLogger(F2SEventSerializer.class);

  /**
   * @see com.esotericsoftware.kryo.Serializer#read(com.esotericsoftware.kryo.Kryo,
   *      com.esotericsoftware.kryo.io.Input, java.lang.Class)
   */
  @Override
  public F2SEvent read(Kryo kryo, Input input, Class<F2SEvent> arg2) {
    return readDirect(kryo, input, arg2);
  }

  private F2SEvent readDirect(Kryo kryo, Input input, Class<F2SEvent> arg2) {
    try {
      F2SEventBuilder builder = new F2SEventBuilder();
      int nbHeaders = input.readInt();
      Builder<String, String> headers = new ImmutableMap.Builder<String, String>();
      for (int i = 0; i < nbHeaders; i++) {
        headers.put(input.readString(), input.readString());
      }
      builder.headers(headers.build());
      int bodySz = input.readInt();
      builder.body(input.readBytes(bodySz));
      // builder.body(kryo.readObject(input, byte[].class));
      return builder.get();
    } catch (Exception e) {
      LOG.error("Failed to deserialize: " + e.getMessage(), e);
      return null;
    }
  }

  @SuppressWarnings("unused")
  private F2SEvent readWithByteBuffer(Kryo kryo, Input input, Class<F2SEvent> arg2) {
    try {
      F2SEventBuilder builder = new F2SEventBuilder();
      int sz = input.readInt();
      byte[] data = input.readBytes(sz);
      ByteBuffer bb = ByteBuffer.wrap(data);
      int nbHeaders = bb.getInt();
      for (int i = 0; i < nbHeaders; i++) {
        int keySz = bb.getInt();
        byte[] key = new byte[keySz];
        bb.get(key);
        int valueSz = bb.getInt();
        byte[] value = new byte[valueSz];
        bb.get(value);
        builder.header(new String(key), new String(value));
      }
      int bodySz = bb.getInt();
      byte[] body = new byte[bodySz];
      bb.get(body);
      builder.body(body);
      return builder.get();
    } catch (Exception e) {
      LOG.error("Failed to deserialize: " + e.getMessage(), e);
      return null;
    }
  }

  /**
   * @see com.esotericsoftware.kryo.Serializer#write(com.esotericsoftware.kryo.Kryo,
   *      com.esotericsoftware.kryo.io.Output, java.lang.Object)
   */
  @Override
  public void write(Kryo kryo, Output output, F2SEvent event) {
    writeDirect(kryo, output, event);
  }

  private void writeDirect(Kryo kryo, Output output, F2SEvent event) {
    try {
      output.writeInt(event.getHeaders().size());
      for (Entry<String, String> header : event.getHeaders().entrySet()) {
        output.writeString(header.getKey());
        output.writeString(header.getValue());
      }
      output.writeInt(event.getBody().length);
      output.writeBytes(event.getBody());
    } catch (Exception e) {
      LOG.error("Failed to serialize!", e);
    }
  }

  @SuppressWarnings("unused")
  private void writeWithByteBuffer(Kryo kryo, Output output, F2SEvent event) {
    try {
      // Computing approximate size of the event
      int sz = 1024 + event.getBody().length;
      for (Entry<String, String> entry : event.getHeaders().entrySet()) {
        sz += entry.getKey().getBytes().length + entry.getValue().getBytes().length;
      }
      ByteBuffer bb = ByteBuffer.allocate(sz);
      bb.putInt(event.getHeaders().size());
      for (Entry<String, String> entry : event.getHeaders().entrySet()) {
        byte[] key = entry.getKey().getBytes();
        byte[] value = entry.getValue().getBytes();
        bb.putInt(key.length);
        bb.put(key);
        bb.putInt(value.length);
        bb.put(value);
      }
      bb.putInt(event.getBody().length);
      bb.put(event.getBody());
      bb.flip();
      byte[] result = Arrays.copyOf(bb.array(), bb.limit());
      output.writeInt(result.length);
      output.writeBytes(result);
    } catch (Exception e) {
      LOG.error("Failed to serialize!", e);
    }
  }
}
