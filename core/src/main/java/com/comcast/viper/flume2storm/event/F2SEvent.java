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

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.collect.ImmutableMap;

/**
 * Event object exchanged between Flume and Storm. We keep it close to Flume's
 * original event.<br/>
 * Programming note: We create this duplicate of Flume event in order to avoid
 * having to bring all Flume dependencies into Storm.
 */
public final class F2SEvent implements Serializable {
  private static final long serialVersionUID = -5906404402187281472L;
  private final Map<String, String> headers;
  private final byte[] body;

  F2SEvent() {
    headers = ImmutableMap.of();
    body = new byte[0];
  }

  /**
   * Constructor to build Flume2Storm events
   * 
   * @param headers
   *          See {@link #getHeaders()}. If null, the headers is an empty
   *          immutable map.
   * @param body
   *          See {@link #getBody()}. If null, the payload of the event is an
   *          empty byte array.
   */
  public F2SEvent(Map<String, String> headers, byte[] body) {
    if (headers == null) {
      this.headers = ImmutableMap.of();
    } else {
      this.headers = ImmutableMap.copyOf(headers);
    }
    if (body == null) {
      this.body = ArrayUtils.EMPTY_BYTE_ARRAY;
    } else {
      this.body = ArrayUtils.clone(body);
    }
  }

  /**
   * Copy constructor
   * 
   * @param event
   *          Another Flume2Storm event
   */
  public F2SEvent(F2SEvent event) {
    headers = ImmutableMap.copyOf(event.getHeaders());
    body = event.body;
  }

  /**
   * @return The event body
   */
  public byte[] getBody() {
    return body;
  }

  /**
   * @return The headers (key/value pairs) associated to the event
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * @param key
   *          A header name (i.e. key)
   * @return The value of the header, or null if it does not exist
   */
  public String getHeader(String key) {
    return headers.get(key);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(headers).append(body).hashCode();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    F2SEvent other = (F2SEvent) obj;
    return new EqualsBuilder().append(this.headers, other.headers)
        .append(this.body, other.body).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    ToStringBuilder toStringBuilder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("headers",
        headers);
    String bodyStr = StringUtils.toEncodedString(body, F2SEventFactory.DEFAULT_CHARACTER_SET);
    if (StringUtils.isAsciiPrintable(bodyStr)) {
      toStringBuilder.append("body", bodyStr);
    } else {
      toStringBuilder.append(Hex.encodeHexString(body));
    }
    return toStringBuilder.toString();
  }
}
