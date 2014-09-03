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
package com.comcast.viper.flume2storm.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for {@link EventConverter}
 */
public class EventConverterTest {
  private static final String TIMESTAMP_HEADER = "MyCustomTimestampHeader";
  private static final byte[] TEST_BODY = "whatever".getBytes();

  private EventConverter converter;

  @Before
  public void init() {
    converter = new EventConverter(TIMESTAMP_HEADER);
  }

  /**
   * Test conversion of a complete event
   */
  @Test
  public void testCompleteEvent() {
    Long timestamp = Instant.now().getMillis();
    Event event = EventBuilder.withBody(TEST_BODY, ImmutableMap.of(TIMESTAMP_HEADER, timestamp.toString()));
    F2SEvent f2sEvent = converter.convert(event);
    assertThat(f2sEvent.getBody()).isEqualTo(event.getBody());
    assertThat(f2sEvent.getTimestamp().getMillis()).isEqualTo(timestamp);
    assertThat(f2sEvent.getHeaders()).isEqualTo(event.getHeaders());
  }

  protected void testInvalidTimestamp(Event event) {
    try {
      final DateTime timestamp = new DateTime();
      DateTimeUtils.setCurrentMillisFixed(timestamp.getMillis());
      F2SEvent f2sEvent = converter.convert(event);
      assertThat(f2sEvent.getBody()).isEqualTo(event.getBody());
      assertThat(f2sEvent.getTimestamp().getMillis()).isEqualTo(timestamp.getMillis());
      assertThat(f2sEvent.getHeaders()).isEqualTo(event.getHeaders());
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

  /**
   * Test conversion of an event missing the timestamp header
   */
  @Test
  public void testTimestampMissing() {
    testInvalidTimestamp(EventBuilder.withBody(TEST_BODY, new HashMap<String, String>()));
  }

  /**
   * Test conversion of an event missing the timestamp header
   */
  @Test
  public void testTimestampFormat() {
    testInvalidTimestamp(EventBuilder.withBody(TEST_BODY, ImmutableMap.of(TIMESTAMP_HEADER, "not-a-number")));
  }
}