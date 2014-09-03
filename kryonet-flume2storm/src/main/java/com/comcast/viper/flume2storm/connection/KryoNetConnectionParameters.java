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
package com.comcast.viper.flume2storm.connection;

import java.io.Serializable;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.google.common.base.Preconditions;

/**
 * Configuration for the KryoNet connection
 */
public class KryoNetConnectionParameters implements ConnectionParameters, Serializable {
  private static final long serialVersionUID = -5178954514610562054L;
  /** Configuration attribute name for {@link #getServerAddress()} */
  public static final String HOSTNAME = "hostname";
  /** Default value for {@value #HOSTNAME} */
  public static final String HOSTNAME_DEFAULT = "localhost";
  /** Configuration attribute name for {@link #getServerPort()} */
  public static final String PORT = "port";
  /** Default value for {@value #PORT} */
  public static final int PORT_DEFAULT = 7000;
  /** Configuration attribute name for {@link #getObjectBufferSize()} */
  public static final String OBJECT_BUFFER_SZ = "object.buffer.size";
  /** Default value for {@value #OBJECT_BUFFER_SZ} */
  public static final int OBJECT_BUFFER_SIZE_DEFAULT = 3024;
  /** Configuration attribute name for {@link #getWriteBufferSize()} */
  public static final String WRITE_BUFFER_SZ = "write.buffer.size";
  /** Default value for {@value #WRITE_BUFFER_SZ} */
  public static final int WRITE_BUFFER_SIZE_DEFAULT = OBJECT_BUFFER_SIZE_DEFAULT * 1000;

  protected String serverAddress;
  protected int serverPort;
  protected int objectBufferSize;
  protected int writeBufferSize;

  /**
   * @param config
   *          The configuration to use
   * @return The newly constructed {@link KryoNetConnectionParameters} based on
   *         the {@link Configuration} specified
   */
  public static KryoNetConnectionParameters from(Configuration config) {
    KryoNetConnectionParameters result = new KryoNetConnectionParameters();
    result.serverAddress = config.getString(HOSTNAME, HOSTNAME_DEFAULT);
    result.serverPort = config.getInt(PORT, PORT_DEFAULT);
    result.objectBufferSize = config.getInt(OBJECT_BUFFER_SZ, OBJECT_BUFFER_SIZE_DEFAULT);
    result.writeBufferSize = config.getInt(WRITE_BUFFER_SZ, WRITE_BUFFER_SIZE_DEFAULT);
    return result;
  }

  /**
   * Constructor that initializes the connection parameters using the defaults
   * settings
   */
  public KryoNetConnectionParameters() {
    serverAddress = HOSTNAME_DEFAULT;
    serverPort = PORT_DEFAULT;
    objectBufferSize = OBJECT_BUFFER_SIZE_DEFAULT;
    writeBufferSize = WRITE_BUFFER_SIZE_DEFAULT;
  }

  /**
   * Copy constructor
   * 
   * @param copy
   *          Another configuration object for KryoNet connection parameters
   */
  public KryoNetConnectionParameters(final KryoNetConnectionParameters copy) {
    serverAddress = copy.serverAddress;
    serverPort = copy.serverPort;
    objectBufferSize = copy.objectBufferSize;
    writeBufferSize = copy.writeBufferSize;
  }

  /**
   * @return Address (or host name) to use for the KryoNet event sender.
   *         Defaults to {@value #HOSTNAME_DEFAULT}
   */
  public String getServerAddress() {
    return serverAddress;
  }

  /**
   * @param address
   *          See {@link #getServerAddress()}
   * @return This object
   */
  public KryoNetConnectionParameters setServerAddress(final String address) {
    // TODO Use regular expression for better validation
    Preconditions.checkArgument(address.length() > 0, "KryoNet server address is missing");
    serverAddress = address;
    return this;
  }

  /**
   * @return Port to use for the KryoNet event sender. Defaults to
   *         {@value #PORT_DEFAULT}
   */
  public int getServerPort() {
    return serverPort;
  }

  /**
   * @param port
   *          See {@link #getServerPort()}
   * @return This object
   */
  public KryoNetConnectionParameters setServerPort(final int port) {
    Preconditions.checkArgument(port > 0 && port < 65535, "KryoNet server port is invalid");
    serverPort = port;
    return this;
  }

  /**
   * @return The connection string (for identification purpose)
   */
  public String getConnectionStr() {
    return serverAddress + ":" + serverPort;
  }

  /**
   * @return KryoNet's object buffer size in bytes, which MUST be large enough
   *         to serialize any object, plus some margin for KryoNet's internal
   *         stuff
   */
  public int getObjectBufferSize() {
    return objectBufferSize;
  }

  /**
   * @param objBufSz
   *          See {@link #getObjectBufferSize()}
   * @return This object
   */
  public KryoNetConnectionParameters setObjectBufferSize(final int objBufSz) {
    Preconditions.checkArgument(objBufSz > 0, "KryoNet object size needs to be strictly positive");
    objectBufferSize = objBufSz;
    return this;
  }

  /**
   * @return KryoNet's write buffer size in bytes, size it well...
   */
  public int getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * @param writeBufferSz
   *          See {@link #getWriteBufferSize()}
   * @return This object
   */
  public KryoNetConnectionParameters setWriteBufferSize(final int writeBufferSz) {
    Preconditions.checkArgument(writeBufferSz > 0, "KryoNet write buffer size needs to be strictly positive");
    writeBufferSize = writeBufferSz;
    return this;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(serverAddress).append(serverPort).append(objectBufferSize)
        .append(writeBufferSize).hashCode();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KryoNetConnectionParameters other = (KryoNetConnectionParameters) obj;
    return new EqualsBuilder().append(this.serverAddress, other.serverAddress)
        .append(this.serverPort, other.serverPort).append(this.objectBufferSize, other.objectBufferSize)
        .append(this.writeBufferSize, other.writeBufferSize).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder("KryoNetConnectionParameters").append("serverAddress", serverAddress)
        .append("serverPort", serverPort).append("objectBufferSize", objectBufferSize)
        .append("writeBufferSize", writeBufferSize).toString();
  }
}
