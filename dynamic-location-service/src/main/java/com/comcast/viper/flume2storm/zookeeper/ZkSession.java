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
package com.comcast.viper.flume2storm.zookeeper;

import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * This is immutable session information
 */
public final class ZkSession {
  protected static final ZkSession NO_SESSION = new ZkSession(null, null, 0, 0);
  protected static final String FULL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private final long creationTime;
  private final Long sessionId;
  private final byte[] sessionPwd;
  private final int timeout;

  /**
   * @param sessionId
   *          The session identifier
   * @param sessionPwd
   *          The session password
   * @param timeout
   *          The negociated session timeout value
   * @return The newly constructed {@link ZkSession}
   */
  public static ZkSession build(final Long sessionId, final byte[] sessionPwd, final int timeout) {
    return new ZkSession(sessionId, sessionPwd, System.currentTimeMillis(), timeout);
  }

  /**
   * @param other
   *          Another Zookeeper session
   * @return A copy of the ZK session specified
   */
  public static ZkSession copyOf(final ZkSession other) {
    return new ZkSession(other);
  }

  /**
   * Default constructor
   * 
   * @param sessionId
   *          The session identifier
   * @param sessionPwd
   *          The session password
   * @param creationTime
   *          The date at which the session was created
   * @param timeout
   *          The negotiated session timeout value
   */
  private ZkSession(final Long sessionId, final byte[] sessionPwd, final long creationTime, final int timeout) {
    this.sessionId = sessionId;
    this.sessionPwd = sessionPwd;
    this.creationTime = creationTime;
    this.timeout = timeout;
  }

  /**
   * Copy constructor
   * 
   * @param other
   *          The other session to copy
   */
  private ZkSession(final ZkSession other) {
    this.sessionId = other.sessionId;
    this.sessionPwd = other.sessionPwd;
    this.creationTime = other.creationTime;
    this.timeout = other.timeout;
  }

  /**
   * @param otherSessionId
   *          A session identifier
   * @return True if the 2 session objects represents the same session
   */
  public boolean sameAs(final long otherSessionId) {
    if (!isSet()) {
      return false;
    }
    assert sessionId != null;
    return sessionId.equals(otherSessionId);
  }

  /**
   * @return True if the session is set (i.e. has an id)
   */
  public boolean isSet() {
    return sessionId != null;
  }

  /**
   * @return The session identifier as a number
   */
  public Long getSessionId() {
    return sessionId;
  }

  /**
   * @return The session identifier as a string
   */
  public String getSessionIdStr() {
    if (sessionId == null) {
      return null;
    }
    return "0x" + Long.toHexString(sessionId);
  }

  /**
   * @return The session password
   */
  public byte[] getSessionPwd() {
    return sessionPwd;
  }

  /**
   * @return The negociated session timeout value
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * @return The session creation time as a number
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @return The session creation time as a string
   */
  public String getCreationTimeStr() {
    return new SimpleDateFormat(FULL_DATE_FORMAT).format(creationTime);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (creationTime ^ (creationTime >>> 32));
    result = prime * result + ((sessionId == null) ? 0 : sessionId.hashCode());
    result = prime * result + Arrays.hashCode(sessionPwd);
    result = prime * result + timeout;
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
    final ZkSession other = (ZkSession) obj;
    if (creationTime != other.creationTime) {
      return false;
    }
    if (sessionId == null) {
      if (other.sessionId != null) {
        return false;
      }
    } else if (!sessionId.equals(other.sessionId)) {
      return false;
    }
    if (!Arrays.equals(sessionPwd, other.sessionPwd)) {
      return false;
    }
    if (timeout != other.timeout) {
      return false;
    }
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ZkSession [");
    if (isSet()) {
      sb.append("Id=");
      sb.append(getSessionIdStr());
      sb.append("; Creation=");
      sb.append(getCreationTimeStr());
      sb.append("; Timeout(ms)=");
      sb.append(timeout);
    } else {
      sb.append("NO_SESSION");
    }
    sb.append("]");
    return sb.toString();
  }
}
