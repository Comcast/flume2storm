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

import java.util.Map;

import junit.framework.Assert;

import org.joda.time.Instant;
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
    Instant timestamp = Instant.now();
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    Assert.assertEquals(new F2SEvent(headers, timestamp, body), new F2SEvent(headers, timestamp, body));
  }
}
