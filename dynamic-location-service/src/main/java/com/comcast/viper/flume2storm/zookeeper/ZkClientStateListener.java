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
 * Notifies the ZkClient of state changes. These are called in a serial fashion
 * (by design).
 */
public interface ZkClientStateListener {
	/**
	 * Note that the state change as already occurred
	 * 
	 * @param previousState
	 *            The state of {@link ZkClient} before the transition
	 * @param newState
	 *            The state of {@link ZkClient} after the transition
	 */
	public void onStateChange(ZkClientState previousState, ZkClientState newState);
}
