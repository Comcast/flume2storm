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
 * This listener notifies of noteworthy ZkClient events. This listener is only
 * for the ZkClient implementation use.
 */
public interface ZkClientListener {
	/**
	 * Called when {@link ZkClientState#INITIALIZING} (that's the actual
	 * initialization). Once performed, the ZkClient is considered
	 * {@link ZkClientState#SETUP}. Note that it should not be assumed that
	 * {@link #terminate()} has been called.
	 */
	public void initialize();

	/**
	 * Called while {@link ZkClientState#DISCONNECTING}. Any clean up actions
	 * before disconnection should execute in less than
	 * {@link ZkClientConfiguration#getTerminationTimeout()} in order to have
	 * time to properly occur. Note however that this call may never be called
	 * (i.e. if the connection is lost before the {@link ZkClient} gets a
	 * chance to execute it).
	 */
	public void terminate();

	/**
	 * Notification of connection event to the ZK quorum. Nothing is required
	 * from the listener - Depending on the state of {@link ZkClient}, it'll
	 * take the appropriate actions.
	 */
	public void onConnection();

	/**
	 * Notification of disconnection event to the ZK quorum. Nothing is required
	 * from the listener - Depending on the state of {@link ZkClient}, it'll
	 * take the appropriate actions.
	 */
	public void onDisconnection();
}
