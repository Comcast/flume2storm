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

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 */
public class TestBolt extends BaseRichBolt {
  private static final long serialVersionUID = -4437924501877270487L;
  protected OutputCollector collector;

  /**
   * @see backtype.storm.task.IBolt#prepare(java.util.Map,
   *      backtype.storm.task.TopologyContext,
   *      backtype.storm.task.OutputCollector)
   */
  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
  }

  /**
   * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
   */
  @Override
  public void execute(Tuple input) {
    final F2SEvent event = (F2SEvent) input
        .getValueByField(BasicF2SEventEmitter.F2S_EVENT);
    MemoryStorage.getInstance().addEvent(event);
    collector.ack(input);
  }

  /**
   * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // This bolt is not emitting anything
  }
}
