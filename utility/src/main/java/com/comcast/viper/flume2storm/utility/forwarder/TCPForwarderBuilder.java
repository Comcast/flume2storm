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
 * Builder class for the {@link TCPForwarder}
 */
public class TCPForwarderBuilder extends TCPForwarderConfig {
	public TCPForwarderBuilder() {
		super();
	}

	public void validate() {
		assert inputPort != null : "TCPForwarder input port not specified";
		assert outputServer != null : "TCPForwarder output server not specified";
		assert outputPort != null : "TCPForwarder output port not specified";
		// TODO check more: not the same port with localhost, port value in
		// range, ...
	}

	/**
	 * @return A newly built {@link TCPForwarder}
	 */
	public TCPForwarder build() {
		validate();
		return new TCPForwarderImpl(this);
	}
}
