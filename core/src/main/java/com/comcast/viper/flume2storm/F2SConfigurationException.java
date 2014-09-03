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

/**
 * Exception when the configuration of a Flume2Storm component is invalid
 */
public class F2SConfigurationException extends Exception {
  private static final long serialVersionUID = 4941423443352427505L;

  // TODO This kind of Strings should be externalized
  public static enum Reason {
    NOT_POSITIVE("It must be positive"),
    NOT_STRICLY_POSITIVE("It must be strictly positive");

    private final String message;

    private Reason(final String message) {
      this.message = message;
    }

    public String getMessage() {
      return message;
    }
  }

  public static F2SConfigurationException with(String parameter, Object value, Throwable throwable) {
    return new F2SConfigurationException(buildErrorMessage(parameter, value, throwable.getMessage()), throwable);
  }

  public static F2SConfigurationException with(String parameter, Object value, Reason reason) {
    return new F2SConfigurationException(buildErrorMessage(parameter, value, reason.getMessage()));
  }

  protected static final String buildErrorMessage(String parameter, Object value, String message) {
    return new StringBuilder("Configuration attribute '").append(parameter).append("' has invalid value (")
        .append(value.toString()).append("): ").append(message).toString();
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
