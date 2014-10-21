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
package com.comcast.viper.flume2storm.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.spout.F2SEventEmitter;

public class ExampleStringEmitter implements F2SEventEmitter {
  private static final long serialVersionUID = 5204406640833581933L;
  public static final Fields fields = new Fields("field");
  private static final Logger LOG = LoggerFactory.getLogger(ExampleStringEmitter.class);
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }

  @Override
  public void emitEvent(F2SEvent event, SpoutOutputCollector collector) {
    final List<Object> tuple = new ArrayList<Object>(1);
    final String bodyString = Arrays.toString(event.getBody());
    LOG.debug("emitting {}", bodyString);
    tuple.add(bodyString);
    collector.emit(tuple);
  }

}
