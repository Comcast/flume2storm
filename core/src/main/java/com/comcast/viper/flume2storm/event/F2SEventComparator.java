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

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;

/**
 * Compares 2 Flume2Storm events, by sorting them by timestamp first, then by
 * their actual content.
 */
public class F2SEventComparator implements Comparator<F2SEvent> {
  /**
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  public int compare(F2SEvent e1, F2SEvent e2) {
    Preconditions.checkNotNull(e1, "Cannot compare null Flume2Storm events");
    Preconditions.checkNotNull(e2, "Cannot compare null Flume2Storm events");
    return ComparisonChain.start().compare(e1.getTimestamp(), e2.getTimestamp())
        .compare(new String(e1.getBody()), new String(e2.getBody())).result();
  }
}
