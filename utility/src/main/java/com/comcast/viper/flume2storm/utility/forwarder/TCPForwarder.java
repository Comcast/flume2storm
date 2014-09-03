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
 * This class implements a TCP forwarder, which accepts connections and
 * replicates it's clients behavior to the specified server. Note that it also
 * replicates the server's behavior to the clients - i.e. if the server closes
 * the socket, it'll close the client socket as well. <br />
 * The intent of this class is to provide a facility to test networking
 * connection between a client and a server. Optionally, delay at various point
 * in the communication can be introduced. Delays can be modified at any time
 * during the lifetime of the {@link TCPForwarder}, and will take effect the
 * next time the appropriate condition happens (connection, client sending data,
 * ...), and subsequently.
 */
public interface TCPForwarder {
	/**
	 * Starts the TCP forwarder. When started, it'll accept connections from
	 * clients
	 */
	public abstract void start();

	/**
	 * Stops the TCP forwarder. On termination, it closes all connections
	 */
	public abstract void stop();

	/**
	 * @return True if the {@link TCPForwarder} is started and active. Note that
	 *         if frozen, it's not considered "active" and therefore returns
	 *         false
	 */
	public abstract boolean isActive();

	/**
	 * Freezes all connections. No more data will be exchanged between the
	 * client and the server on established connections. Moreover, no client
	 * connection are accepted when frozen. Call {@link #resume()} to resume
	 * normal operation
	 */
	public abstract void freeze();

	/**
	 * @return True if the {@link TCPForwarder} is in the frozen state
	 */
	public abstract boolean isFrozen();

	/**
	 * Resumes normal operation after a call to {@link #freeze()}
	 */
	public abstract void resume();

	//
	// Delay management
	//

	/**
	 * Resets (i.e. set to 0) all the delay
	 */
	public abstract void resetDelay();

	/**
	 * @return The additional delay in milliseconds added while the connection
	 *         is being established. A word of caution though: connection delay
	 *         prevents other clients to connect to the {@link TCPForwarder}
	 */
	public abstract int getConnectionDelay();

	/**
	 * @param delay
	 *            See {@link #getConnectionDelay()}
	 * @return This object
	 */
	public abstract TCPForwarder setConnectionDelay(int delay);

	/**
	 * @return The additional delay in milliseconds added after the client sent
	 *         data to the server but before the server received it from the
	 *         client.
	 */
	public abstract int getClientSendDelay();

	/**
	 * @param delay
	 *            See {@link #getClientSendDelay()}
	 * @return This object
	 */
	public abstract TCPForwarder setClientSendDelay(int delay);

	/**
	 * @return The additional delay in milliseconds added after the server sent
	 *         data to the client but before the client received it from the
	 *         server.
	 */
	public abstract int getServerSendDelay();

	/**
	 * @param delay
	 *            See {@link #getServerSendDelay()}
	 * @return This object
	 */
	public abstract TCPForwarder setServerSendDelay(int delay);
}
