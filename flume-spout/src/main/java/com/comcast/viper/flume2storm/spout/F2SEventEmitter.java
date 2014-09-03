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
package com.comcast.viper.flume2storm.spout;

import java.io.Serializable;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;

import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * Interface to connect the Flume-Spout to the rest of the topology
 */
public interface F2SEventEmitter extends Serializable {
  /**
   * Implement this method to specify what tuples the Flume-Spout will emit on
   * which stream.
   * 
   * @param declarer
   *          Storm {@link OutputFieldsDeclarer}
   */
  public void declareOutputFields(OutputFieldsDeclarer declarer);

  /**
   * Method to actually emit the Flume2Storm events down the topology. Tuples
   * must match what has been declared in
   * {@link #declareOutputFields(OutputFieldsDeclarer)}.
   * 
   * @param event
   *          The event being emitted
   * @param collector
   *          The collector to use
   */
  public void emitEvent(F2SEvent event, SpoutOutputCollector collector);

  // TODO Add acknowledgement methods
}
