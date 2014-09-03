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

import com.comcast.viper.flume2storm.connection.KryoNetConnectionParameters;
import com.google.common.base.Preconditions;

/**
 * Serializes and deserializes a {@link KryoNetServiceProvider}
 */
public class KryoNetServiceProviderSerialization implements ServiceProviderSerialization<KryoNetServiceProvider> {
  private static final long serialVersionUID = -3054825185354578392L;
  protected static String SEPARATOR = ":";

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProviderSerialization#serialize(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  @Override
  public byte[] serialize(KryoNetServiceProvider serviceProvider) {
    KryoNetConnectionParameters cp = serviceProvider.getConnectionParameters();
    StringBuilder sb = new StringBuilder();
    sb.append(cp.getServerAddress());
    sb.append(SEPARATOR);
    sb.append(cp.getServerPort());
    sb.append(SEPARATOR);
    sb.append(cp.getObjectBufferSize());
    sb.append(SEPARATOR);
    sb.append(cp.getWriteBufferSize());
    return sb.toString().getBytes();
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProviderSerialization#deserialize(byte[])
   */
  @Override
  public KryoNetServiceProvider deserialize(byte[] bytes) {
    Preconditions.checkNotNull(bytes, "Failed to deserialize KryoNet Service Provider: null byte array");
    String[] elements = new String(bytes).split(SEPARATOR);
    Preconditions.checkArgument(elements.length == 4,
        "Failed to deserialize KryoNet Service Provider: invalid number of elements");
    KryoNetConnectionParameters cp = new KryoNetConnectionParameters();
    cp.setServerAddress(elements[0]);
    cp.setServerPort(Integer.parseInt(elements[1]));
    cp.setObjectBufferSize(Integer.parseInt(elements[2]));
    cp.setWriteBufferSize(Integer.parseInt(elements[3]));
    return new KryoNetServiceProvider(cp);
  }
}
