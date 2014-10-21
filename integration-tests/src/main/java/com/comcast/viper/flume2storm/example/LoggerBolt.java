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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class LoggerBolt extends BaseBasicBolt {
  private static final long serialVersionUID = -5470303823148808079L;
  private static final Logger LOG = LoggerFactory.getLogger(LoggerBolt.class);
  @Override
  public void execute(Tuple arg0, BasicOutputCollector arg1) {
    LOG.debug("EXECUTING!!!");
    LOG.debug(arg0.toString());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {
  }

}
