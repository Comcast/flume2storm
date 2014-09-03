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

import java.util.HashMap;
import java.util.Map;

import org.joda.time.Instant;

import com.google.common.base.Supplier;

/**
 * Builds a Flume2Storm event.
 */
public class F2SEventBuilder implements Supplier<F2SEvent> {
  private Map<String, String> headers;
  private Instant timestamp;
  private byte[] body;

  /**
   * @return See {@link F2SEvent#getTimestamp()}
   */
  public Instant getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp
   *          See {@link F2SEvent#getTimestamp()}
   * @return This builder object
   */
  public F2SEventBuilder timestamp(Instant timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * @return See {@link F2SEvent#getBody()}
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * @param body
   *          See {@link F2SEvent#getBody()}
   * @return This builder object
   */
  public F2SEventBuilder body(byte[] body) {
    this.body = body;
    return this;
  }

  /**
   * @return See {@link F2SEvent#getHeaders()}
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * @param headers
   *          See {@link F2SEvent#getHeaders()}
   * @return This builder object
   */
  public F2SEventBuilder headers(Map<String, String> headers) {
    this.headers = headers;
    return this;
  }

  /**
   * Adds the specified header
   * 
   * @param key
   *          The header key
   * @param value
   *          The header value
   */
  public void header(String key, String value) {
    if (headers == null) {
      headers = new HashMap<String, String>();
    }
    headers.put(key, value);
  }

  /**
   * @see com.google.common.base.Supplier#get()
   */
  public F2SEvent get() {
    return new F2SEvent(headers, timestamp, body);
  }
}
