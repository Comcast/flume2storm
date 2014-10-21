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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters;
import com.google.common.base.Preconditions;

/**
 * Configuration for the KryoNet connection. Note that the hostname of the
 * server is configured according to the following algorithm:
 * <ol>
 * <li>If the {@link #ADDRESS} parameter is specified, this is the
 * address/hostname that is used</li>
 * <li>Otherwise, the IP address of the host is retrieved and used</li>
 * <li>If the previous step failed, the default hostname
 * {@link #ADDRESS_DEFAULT} is used</li>
 * </ol>
 */
public class KryoNetConnectionParameters implements ConnectionParameters, Serializable {
  private static final long serialVersionUID = -5178954514610562054L;
  /** Configuration attribute name for {@link #getAddress()} */
  public static final String ADDRESS = "address";
  /** Default value for {@value #ADDRESS} */
  public static final String ADDRESS_DEFAULT = "localhost";
  /** Configuration attribute name for {@link #getPort()} */
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

  protected String address;
  protected int port;
  protected int objectBufferSize;
  protected int writeBufferSize;

  /**
   * @param config
   *          The configuration to use
   * @return The newly constructed {@link KryoNetConnectionParameters} based on
   *         the {@link Configuration} specified
   * @throws F2SConfigurationException
   *           If the configuration specified is invalid
   */
  public static KryoNetConnectionParameters from(Configuration config) throws F2SConfigurationException {
    KryoNetConnectionParameters result = new KryoNetConnectionParameters();
    try {
      String hostname = config.getString(ADDRESS);
      if (hostname == null) {
        try {
          hostname = InetAddress.getLocalHost().getHostAddress();
        } catch (final UnknownHostException e) {
          hostname = ADDRESS_DEFAULT;
        }
      }
      assert hostname != null;
      result.setAddress(hostname);
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, ADDRESS, e);
    }
    try {
      result.setServerPort(config.getInt(PORT, PORT_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, PORT, e);
    }
    try {
      result.setObjectBufferSize(config.getInt(OBJECT_BUFFER_SZ, OBJECT_BUFFER_SIZE_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, OBJECT_BUFFER_SZ, e);
    }
    try {
      result.setWriteBufferSize(config.getInt(WRITE_BUFFER_SZ, WRITE_BUFFER_SIZE_DEFAULT));
    } catch (Exception e) {
      throw F2SConfigurationException.with(config, WRITE_BUFFER_SZ, e);
    }
    return result;
  }

  /**
   * Constructor that initializes the connection parameters using the defaults
   * settings
   */
  public KryoNetConnectionParameters() {
    address = ADDRESS_DEFAULT;
    port = PORT_DEFAULT;
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
    address = copy.address;
    port = copy.port;
    objectBufferSize = copy.objectBufferSize;
    writeBufferSize = copy.writeBufferSize;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.parameters.ConnectionParameters#getId()
   */
  @Override
  public String getId() {
    return new StringBuilder().append(address).append(":").append(port).toString();
  }

  /**
   * @return Address (or host name) to use for the KryoNet event sender.
   *         Defaults to {@value #ADDRESS_DEFAULT}
   */
  public String getAddress() {
    return address;
  }

  /**
   * @param address
   *          See {@link #getAddress()}
   * @return This object
   */
  public KryoNetConnectionParameters setAddress(final String address) {
    Preconditions.checkArgument(address.length() > 0, "KryoNet server address is missing");
    this.address = address;
    return this;
  }

  /**
   * @return Port to use for the KryoNet event sender. Defaults to
   *         {@value #PORT_DEFAULT}
   */
  public int getPort() {
    return port;
  }

  /**
   * @param port
   *          See {@link #getPort()}
   * @return This object
   */
  public KryoNetConnectionParameters setServerPort(final int port) {
    Preconditions.checkArgument(port > 0 && port <= 65535, "KryoNet server port is invalid");
    this.port = port;
    return this;
  }

  /**
   * @return The connection string (for identification purpose)
   */
  public String getConnectionStr() {
    return address + ":" + port;
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
    return new HashCodeBuilder().append(address).append(port).append(objectBufferSize).append(writeBufferSize)
        .hashCode();
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
    return new EqualsBuilder().append(this.address, other.address).append(this.port, other.port)
        .append(this.objectBufferSize, other.objectBufferSize).append(this.writeBufferSize, other.writeBufferSize)
        .isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("address", address).append("port", port)
        .append("objectBufferSize", objectBufferSize).append("writeBufferSize", writeBufferSize).toString();
  }
}
