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
 * Unit test for Flume2Storm event comparator
 */
public final class TestF2SEventComparator {
  /**
   * Test the comparison of 2 identical Flume2Storm events
   */
  @Test
  public void testSame() {
    byte[] body = "Test string".getBytes();
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    Instant timestamp = Instant.now();
    int res = new F2SEventComparator().compare(new F2SEvent(headers, timestamp, body), new F2SEvent(headers, timestamp,
        body));
    Assert.assertTrue(res == 0);
  }

  /**
   * Test the comparison of Flume2Storm events where the body is different
   */
  @Test
  public void testHeader() {
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    Instant timestamp = Instant.now();
    int res = new F2SEventComparator().compare(new F2SEvent(headers, timestamp, "1".getBytes()), new F2SEvent(headers,
        timestamp, "2".getBytes()));
    Assert.assertTrue(res < 0);
  }

  /**
   * Test the comparison of Flume2Storm events where the timestamp is different
   */
  @Test
  public void testTimestamp() {
    byte[] body = "Test string".getBytes();
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    long t0 = System.currentTimeMillis();
    int res = new F2SEventComparator().compare(new F2SEvent(headers, new Instant(t0), body), new F2SEvent(headers,
        new Instant(t0 + 1), body));
    Assert.assertTrue(res < 0);
  }

  /**
   * Test the comparison of Flume2Storm events where the timestamp and body are
   * different
   */
  @Test
  public void testTimestampPrecedence() {
    Map<String, String> headers = ImmutableMap.of("K1", "v1", "k2", "v2");
    long t0 = System.currentTimeMillis();
    int res = new F2SEventComparator().compare(new F2SEvent(headers, new Instant(t0), "2".getBytes()), new F2SEvent(
        headers, new Instant(t0 + 1), "1".getBytes()));
    Assert.assertTrue(res < 0);
  }
}
