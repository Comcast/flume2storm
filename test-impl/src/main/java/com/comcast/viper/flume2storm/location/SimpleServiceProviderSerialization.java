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

import com.google.common.base.Preconditions;

/**
 * A simple implementation of ServiceProviderSerialization for test/example
 * purpose
 */
public class SimpleServiceProviderSerialization implements ServiceProviderSerialization<SimpleServiceProvider> {
  private static final long serialVersionUID = -1002890263399098547L;
  private static final String SEPARATOR = ":";

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProviderSerialization#serialize(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public byte[] serialize(SimpleServiceProvider serviceProvider) {
    Preconditions.checkNotNull(serviceProvider, "Failed to serialize null SimpleServiceProvider");
    return new StringBuilder().append(serviceProvider.getHostname()).append(SEPARATOR)
        .append(serviceProvider.getPort()).toString().getBytes();
  }

  /**
   * @see com.comcast.viper.flume2storm.location.ServiceProviderSerialization#deserialize(byte[])
   */
  public SimpleServiceProvider deserialize(byte[] bytes) {
    Preconditions.checkNotNull(bytes, "Failed to deserialize null SimpleServiceProvider");
    String string = new String(bytes);
    String[] elements = string.split(SEPARATOR);
    if (elements.length != 2) {
      throw new IllegalArgumentException("Failed to deserialize SimpleServiceProvider: " + string);
    }
    return new SimpleServiceProvider(elements[0], Integer.parseInt(elements[1]));
  }
}
