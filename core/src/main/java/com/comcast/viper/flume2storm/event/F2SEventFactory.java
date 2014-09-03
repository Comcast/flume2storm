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
package com.comcast.viper.flume2storm.event;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

/**
 * Flume2Storm event factory.
 */
public class F2SEventFactory {
  static final String DEFAULT_CHARACTER_SET_NAME = "UTF-8";
  static final Charset DEFAULT_CHARACTER_SET = Charset.forName(DEFAULT_CHARACTER_SET_NAME);
  private static final F2SEventFactory INSTANCE = new F2SEventFactory();
  private final Random random;

  /**
   * @return The instance of the Flume2Storm event factory
   */
  public static F2SEventFactory getInstance() {
    return INSTANCE;
  }

  private F2SEventFactory() {
    random = new Random();
  }

  /**
   * @param body
   *          The payload of the event
   * @return The newly created Flume2Storm event
   */
  public F2SEvent create(byte[] body) {
    return new F2SEvent(null, null, body);
  }

  /**
   * @param body
   *          The payload of the event
   * @return The newly created Flume2Storm event
   */
  public F2SEvent create(String body) {
    return new F2SEvent(null, null, body.getBytes(DEFAULT_CHARACTER_SET));
  }

  /**
   * @param body
   *          The payload of the event
   * @param headers
   *          The headers associated with the event
   * @return The newly created Flume2Storm event
   */
  public F2SEvent create(byte[] body, Map<String, String> headers) {
    return new F2SEvent(headers, null, body);
  }

  /**
   * @return An event with a payload containing a random alpha-numeric string
   *         (and no headers)
   */
  public F2SEvent createRandom() {
    return create(RandomStringUtils.randomAlphanumeric(random.nextInt(128) + 1));
  }

  /**
   * @return An event with a payload containing a random alpha-numeric string
   *         (and no headers)
   */
  public F2SEvent createRandomWithHeaders() {
    F2SEventBuilder builder = new F2SEventBuilder();
    for (int i = 0; i < random.nextInt(5); i++) {
      builder.header("H" + i, RandomStringUtils.randomAlphabetic(random.nextInt(64) + 1));
    }
    builder.body(RandomStringUtils.randomAlphanumeric(random.nextInt(128) + 1)
        .getBytes());
    return builder.get();
  }
}
