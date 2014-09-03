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

import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.comcast.viper.flume2storm.F2SEventSerializer;
import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 * Basic implementation of the Flume2Storm emitter. It emits the full Flume
 * event (timestamp, header and content) on the default stream.<br/>
 * Programming note: Make sure you use the {@link F2SEventSerializer} from
 * kryonet-flume2storm to serialize the {@link F2SEvent}
 */
public class BasicF2SEventEmitter implements F2SEventEmitter {
  private static final long serialVersionUID = 8195071400428813016L;
  /** Field name of emitted tuples */
  public static final String F2S_EVENT = "f2sEvent";
  /** Tuple fields declaration */
  public static final Fields fields = new Fields(F2S_EVENT);

  /**
   * @see com.comcast.viper.flume2storm.spout.F2SEventEmitter#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }

  /**
   * @see com.comcast.viper.flume2storm.spout.F2SEventEmitter#emitEvent(com.comcast.viper.flume2storm.event.F2SEvent,
   *      backtype.storm.spout.SpoutOutputCollector)
   */
  @Override
  public void emitEvent(F2SEvent event, SpoutOutputCollector collector) {
    final List<Object> tuple = new ArrayList<Object>(1);
    tuple.add(event);
    collector.emit(tuple);
  }
}
