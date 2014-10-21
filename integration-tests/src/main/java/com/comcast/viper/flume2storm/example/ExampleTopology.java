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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.KryoNetConnectionParameters;
import com.comcast.viper.flume2storm.location.KryoNetServiceProvider;
import com.comcast.viper.flume2storm.spout.FlumeSpout;

public class ExampleTopology {
  public static void main(String[] args) throws F2SConfigurationException, ConfigurationException, AlreadyAliveException, InvalidTopologyException {
    Configuration configuration = new PropertiesConfiguration("properties");
    ExampleStringEmitter emitter = new ExampleStringEmitter();
    FlumeSpout<KryoNetConnectionParameters,KryoNetServiceProvider> spout = new FlumeSpout<KryoNetConnectionParameters,KryoNetServiceProvider>(emitter, configuration);
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout", spout);
    LoggerBolt bolt = new LoggerBolt();
    builder.setBolt("bolt", bolt).shuffleGrouping("spout");
    
    Config stormConf = new Config();
    stormConf.setDebug(true);
    stormConf.setNumWorkers(3);
    StormSubmitter.submitTopology("ExampleTopology", stormConf, builder.createTopology());
  }
}
