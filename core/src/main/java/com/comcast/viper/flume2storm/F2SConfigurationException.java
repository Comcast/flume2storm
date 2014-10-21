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
package com.comcast.viper.flume2storm;

import org.apache.commons.configuration.Configuration;

/**
 * Exception when the configuration of a Flume2Storm component is invalid
 */
@SuppressWarnings("javadoc")
public class F2SConfigurationException extends Exception {
  private static final long serialVersionUID = 4941423443352427505L;

  /**
   * @param config
   *          The configuration that contains the invalid parameter
   * @param parameter
   *          The name of the configuration parameter that is invalid
   * @param throwable
   *          The exception that occurred when setting the parameter's value
   * @return The newly built F2SConfigurationException related to a specific
   *         invalid parameter
   */
  public static F2SConfigurationException with(Configuration config, String parameter, Throwable throwable) {
    return F2SConfigurationException.with(parameter, config.getProperty(parameter), throwable);
  }

  /**
   * @param parameter
   *          The name of the configuration parameter that is invalid
   * @param value
   *          The configured value of the parameter
   * @param throwable
   *          The exception that occurred when setting the parameter's value
   * @return The newly built F2SConfigurationException related to a specific
   *         invalid parameter
   */
  public static F2SConfigurationException with(String parameter, Object value, Throwable throwable) {
    return new F2SConfigurationException(buildErrorMessage(parameter, value, throwable), throwable);
  }

  protected static final String buildErrorMessage(String parameter, Object value, Throwable throwable) {
    return new StringBuilder("Configuration attribute \"").append(parameter).append("\" has invalid value (\"")
        .append(value).append("\"). ").append(throwable.getClass().getSimpleName()).append(": ")
        .append(throwable.getLocalizedMessage()).toString();
  }

  /**
   * @param parameter
   *          The name of the configuration parameter that is missing
   * @return The newly built F2SConfigurationException related to a specific
   *         missing parameter
   */
  public static F2SConfigurationException missing(String parameter) {
    return new F2SConfigurationException(new StringBuilder("Configuration attribute \"").append(parameter)
        .append("\" is required but not specified").toString());
  }

  public F2SConfigurationException() {
    super();
  }

  public F2SConfigurationException(String message) {
    super(message);
  }

  public F2SConfigurationException(Throwable throwable) {
    super(throwable);
  }

  public F2SConfigurationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
