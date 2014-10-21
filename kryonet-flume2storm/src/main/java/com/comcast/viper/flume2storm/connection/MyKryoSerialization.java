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
package com.comcast.viper.flume2storm.connection;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage.DiscoverHost;
import com.esotericsoftware.kryonet.FrameworkMessage.KeepAlive;
import com.esotericsoftware.kryonet.FrameworkMessage.Ping;
import com.esotericsoftware.kryonet.FrameworkMessage.RegisterTCP;
import com.esotericsoftware.kryonet.FrameworkMessage.RegisterUDP;
import com.esotericsoftware.kryonet.KryoSerialization;

/**
 * This {@link KryoSerialization} for KryoNet is used instead of the default
 * implementation because the later implementation throws a KryoException when
 * receiving data larger than 512 bytes.
 * 
 * <pre>
 * com.esotericsoftware.kryo.KryoException: Buffer underflow.
 *   at com.esotericsoftware.kryo.io.Input.require(Input.java:156) ~[kryo-2.21.jar:na]
 *   at com.esotericsoftware.kryo.io.Input.readBytes(Input.java:317) ~[kryo-2.21.jar:na]
 *   at com.esotericsoftware.kryo.io.Input.readBytes(Input.java:297) ~[kryo-2.21.jar:na]
 *   at com.esotericsoftware.kryo.serializers.DefaultArraySerializers$ByteArraySerializer.read(DefaultArraySerializers.java:35) ~[kryo-2.21.jar:na]
 *   at com.esotericsoftware.kryo.serializers.DefaultArraySerializers$ByteArraySerializer.read(DefaultArraySerializers.java:18) ~[kryo-2.21.jar:na]
 *   at com.esotericsoftware.kryo.Kryo.readObject(Kryo.java:629) [kryo-2.21.jar:na]
 * </pre>
 * 
 * See KryoNetBufferUnderflowTest in the test package of this module
 */
public class MyKryoSerialization extends KryoSerialization {
  private final Kryo kryo;
  private final Input input;
  private final Output output;
  private final ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream();
  private final ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream();

  public MyKryoSerialization(int objectSize) {
    this(new Kryo(), objectSize);
    kryo.setReferences(false);
    kryo.setRegistrationRequired(true);
  }

  public MyKryoSerialization(Kryo kryo, int objectSize) {
    this.kryo = kryo;

    kryo.register(RegisterTCP.class);
    kryo.register(RegisterUDP.class);
    kryo.register(KeepAlive.class);
    kryo.register(DiscoverHost.class);
    kryo.register(Ping.class);

    input = new Input(byteBufferInputStream, objectSize);
    output = new Output(byteBufferOutputStream, objectSize);
  }

  public Kryo getKryo() {
    return kryo;
  }

  public synchronized void write(Connection connection, ByteBuffer buffer, Object object) {
    byteBufferOutputStream.setByteBuffer(buffer);
    kryo.getContext().put("connection", connection);
    kryo.writeClassAndObject(output, object);
    output.flush();
  }

  public synchronized Object read(Connection connection, ByteBuffer buffer) {
    byteBufferInputStream.setByteBuffer(buffer);
    input.setInputStream(byteBufferInputStream);
    kryo.getContext().put("connection", connection);
    return kryo.readClassAndObject(input);
  }

  public void writeLength(ByteBuffer buffer, int length) {
    buffer.putInt(length);
  }

  public int readLength(ByteBuffer buffer) {
    return buffer.getInt();
  }

  public int getLengthLength() {
    return 4;
  }
}
