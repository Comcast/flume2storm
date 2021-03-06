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
package com.comcast.viper.flume2storm.connection.parameters;

import org.apache.commons.configuration.Configuration;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParametersFactory;

/**
 * Implementation of the Connection Parameters factory for KryoNet
 */
public class KryoNetConnectionParametersFactory implements ConnectionParametersFactory<KryoNetConnectionParameters> {
  /** Configuration attribute base name */
  public static final String CONFIG_BASE_NAME = "kryonet.connection.parameters";

  /**
   * @see com.comcast.viper.flume2storm.connection.parameters.ConnectionParametersFactory#create(org.apache.commons.configuration.Configuration)
   */
  @Override
  public KryoNetConnectionParameters create(Configuration config) throws F2SConfigurationException {
    return KryoNetConnectionParameters.from(config.subset(CONFIG_BASE_NAME));
  }
}
