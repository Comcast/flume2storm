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
package com.comcast.viper.flume2storm.utility.forwarder;

/**
 * The configuration for the {@link TCPForwarder}
 */
public class TCPForwarderConfig {
  protected static final int MAX_WORKER_THREAD_DEFAULT = 10;
  protected String listenAddress;
  protected Integer inputPort;
  protected String outputServer;
  protected Integer outputPort;
  protected int maxWorkerThread = MAX_WORKER_THREAD_DEFAULT;

  /**
   * @param config
   *          A {@link TCPForwarder} configuration
   * @return A copy of the configuration specified
   */
  public static TCPForwarderConfig copyOf(final TCPForwarderConfig config) {
    final TCPForwarderConfig result = new TCPForwarderConfig();
    result.inputPort = config.inputPort;
    result.outputServer = config.outputServer;
    result.outputPort = config.outputPort;
    return result;
  }

  TCPForwarderConfig() {
    // no direct instantiation of this
  }

  /**
   * @return The IP address that the {@link TCPForwarder} listens onto for
   *         incoming connections. If null, it listens on all available
   *         interfaces
   */
  public String getListenAddress() {
    return listenAddress;
  }

  /**
   * @param listenAddress
   *          See {@link #getListenAddress()}
   * @return This configurable object
   */
  public TCPForwarderConfig setListenAddress(final String listenAddress) {
    this.listenAddress = listenAddress;
    return this;
  }

  /**
   * @return The port that the {@link TCPForwarder} listens to for incoming
   *         connection
   */
  public Integer getInputPort() {
    return inputPort;
  }

  /**
   * @param inputPort
   *          See {@link #getInputPort()}
   * @return This configurable object
   */
  public TCPForwarderConfig setInputPort(final int inputPort) {
    this.inputPort = inputPort;
    return this;
  }

  /**
   * @return The hostname that the {@link TCPForwarder} connects for outgoing
   *         connections
   */
  public String getOutputServer() {
    return outputServer;
  }

  /**
   * @param outputServer
   *          See {@link #getOutputServer()}
   * @return This configurable object
   */
  public TCPForwarderConfig setOutputServer(final String outputServer) {
    this.outputServer = outputServer;
    return this;
  }

  /**
   * @return The port that the {@link TCPForwarder} uses to connect for outgoing
   *         connections
   */
  public Integer getOutputPort() {
    return outputPort;
  }

  /**
   * @param outputPort
   *          See {@link #getOutputPort()}
   * @return This configurable object
   */
  public TCPForwarderConfig setOutputPort(final int outputPort) {
    this.outputPort = outputPort;
    return this;
  }

  /**
   * @return The maximum number of worker threads that the {@link TCPForwarder}
   *         will create to process incoming connections forwarding. This
   *         defaults to {@value #MAX_WORKER_THREAD_DEFAULT}.
   */
  public int getMaxWorkerThread() {
    return maxWorkerThread;
  }

  /**
   * @param maxWorkerThread
   *          See {@link #getMaxWorkerThread()}
   * @return This configurable object
   */
  public TCPForwarderConfig setMaxWorkerThread(final int maxWorkerThread) {
    this.maxWorkerThread = maxWorkerThread;
    return this;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((inputPort == null) ? 0 : inputPort.hashCode());
    result = prime * result + ((listenAddress == null) ? 0 : listenAddress.hashCode());
    result = prime * result + maxWorkerThread;
    result = prime * result + ((outputPort == null) ? 0 : outputPort.hashCode());
    result = prime * result + ((outputServer == null) ? 0 : outputServer.hashCode());
    return result;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TCPForwarderConfig other = (TCPForwarderConfig) obj;
    if (inputPort == null) {
      if (other.inputPort != null) {
        return false;
      }
    } else if (!inputPort.equals(other.inputPort)) {
      return false;
    }
    if (listenAddress == null) {
      if (other.listenAddress != null) {
        return false;
      }
    } else if (!listenAddress.equals(other.listenAddress)) {
      return false;
    }
    if (maxWorkerThread != other.maxWorkerThread) {
      return false;
    }
    if (outputPort == null) {
      if (other.outputPort != null) {
        return false;
      }
    } else if (!outputPort.equals(other.outputPort)) {
      return false;
    }
    if (outputServer == null) {
      if (other.outputServer != null) {
        return false;
      }
    } else if (!outputServer.equals(other.outputServer)) {
      return false;
    }
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "TCPForwarderConfig [listenAddress=" + listenAddress + ", inputPort=" + inputPort + ", outputServer="
        + outputServer + ", outputPort=" + outputPort + ", maxWorkerThread=" + maxWorkerThread + "]";
  }
}
