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


/**
 * The state diagram of the {@link ZkClient} is unfortunately fairly
 * complicated. The usage of the state enumerate values is actually discouraged:
 * Helper methods are provided for easier management.
 */
public enum ZkClientState {
	/** The ZkClient is inactive, waiting to be started */
	STOPPED,
	/** The ZkClient has been started, and is connecting the ZK quorum */
	CONNECTING,
	/**
	 * The ZkClient received a notification that it actually connected the ZK
	 * quorum
	 */
	CONNECTED,
	/**
	 * After connecting the ZK quorum, if the previous session is expired (or if
	 * it doesn't exist), it's initializing
	 */
	INITIALIZING,
	/**
	 * After connecting the ZK quorum, if the previous session is not expired,
	 * it's recovering the session
	 */
	RECOVERING,
	/** Everything's good, ZkClient is connected to the ZK quorum */
	SETUP,
	/** Before disconnection, clean up - the opposite of {@link #INITIALIZING} */
	CLEANING_UP,
	/**
	 * On request to disconnect, ZkClient is properly shutting down the
	 * connection to ZK quorum - This is obviously the prefered wait to exit
	 */
	DISCONNECTING,
	/**
	 * On connection lost for all state (except for the connecting state, for
	 * which is would be a connection establishement failure), ZkClient is
	 * disconnected from ZK quorum
	 */
	DISCONNECTED;

	/**
	 * @return Whether the {@link ZkClient} is started
	 */
	public boolean isStarted() {
		return this != STOPPED;
	}

	/**
	 * @return Whether the {@link ZkClient} is connected to the Zookeeper
	 *         quorum
	 */
	public boolean isConnected() {
		return this != STOPPED && this != CONNECTING && this != DISCONNECTED;
	}

	/**
	 * @return Whether the {@link ZkClient} is connected to the Zookeeper
	 *         quorum and setup
	 */
	public boolean isSetup() {
		return this == SETUP;
	}
}
