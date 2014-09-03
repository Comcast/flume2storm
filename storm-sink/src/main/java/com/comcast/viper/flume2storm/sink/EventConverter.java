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

import org.apache.flume.Event;
import org.apache.flume.event.EventHelper;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.event.F2SEventBuilder;

/**
 */
public class EventConverter {
  private static final Logger LOG = LoggerFactory.getLogger(EventConverter.class);
  private final String timestampHeader;

  /**
   * @param timestampHeader
   *          See {@link #getTimestampHeader()}
   */
  public EventConverter(String timestampHeader) {
    this.timestampHeader = timestampHeader;
  }

  /**
   * @return The name of the header that contains the timestamp of the event
   */
  public String getTimestampHeader() {
    return timestampHeader;
  }

  /**
   * Convert from Flume event to flume2storm event
   * 
   * @param event
   *          A Flume event
   * @return The corresponding flume2storm event
   */
  public F2SEvent convert(Event event) {
    F2SEventBuilder builder = new F2SEventBuilder().body(event.getBody()).headers(event.getHeaders());
    Long timestamp = null;
    try {
      String ts = event.getHeaders().get(timestampHeader);
      if (ts != null) {
        timestamp = Long.parseLong(ts);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Event does not contain a usable timestamp header: {}", EventHelper.dumpEvent(event));
        }
      }
    } catch (Exception e) {
      LOG.info("Failed to convert event {}: {}", EventHelper.dumpEvent(event), e.getMessage());
    }
    builder.timestamp(new Instant(timestamp));
    return builder.get();
  }
}
