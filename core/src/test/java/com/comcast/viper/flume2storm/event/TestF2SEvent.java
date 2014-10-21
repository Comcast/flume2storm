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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Unit test for Flume2Storm event
 */
public final class TestF2SEvent {
  /**
   * Test that 2 identical events are equals
   */
  @Test
  public void testEquals() {
    byte[] body = "Test string".getBytes();
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    Assert.assertEquals(new F2SEvent(headers, body), new F2SEvent(headers, body));
  }

  /**
   * Test toString method
   */
  @Test
  public void testToString() {
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");

    // Printable event payload
    String bodyStr = "Test string 1234";
    System.out.println(new F2SEvent(headers, bodyStr.getBytes()).toString());
    assertThat(new F2SEvent(headers, bodyStr.getBytes()).toString()).contains(bodyStr);

    // Non-printable payload
    byte[] data = Arrays.copyOf(bodyStr.getBytes(), bodyStr.length() + 1);
    System.out.println(new F2SEvent(headers, data).toString());
    assertThat(new F2SEvent(headers, data).toString()).doesNotContain(bodyStr);
  }
}
