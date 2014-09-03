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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.lang.StringUtils;

import com.comcast.viper.flume2storm.utility.test.TestCondition;
import com.google.common.base.Preconditions;

/**
 * 
 * 
 * @author gcommeau
 */
public class ZkTestUtils {
	public static final int TEST_TIMEOUT = 5000;
	public static final int ZK_OP_TIMEOUT = 3000;
	public static final int A_LITTLE = 1500;

	public static final String HOST = "127.0.0.1";
	public static final int PORT = 2182;
	public static final String ZK_OK_CMD = "ruok";

	// This method is copied from Flume OG's code
	/**
	 * Utility function to send a command to the internal server. ZK servers
	 * accepts 4 byte command strings to test their liveness.
	 */
	protected static String send4LetterWord(final String host, final int port, final String cmd) {
		Preconditions.checkArgument(cmd.length() == 4);
		try {
			final Socket sock = new Socket(host, port);
			OutputStream outstream = null;
			BufferedReader reader = null;
			try {
				outstream = sock.getOutputStream();
				outstream.write(cmd.getBytes());
				outstream.flush();

				reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				final StringBuilder sb = new StringBuilder();
				String line;
				while ((line = reader.readLine()) != null) {
					sb.append(line + "\n");
				}
				return sb.toString();
			} finally {
				if (outstream != null) {
					outstream.close();
				}
				sock.close();
				if (reader != null) {
					reader.close();
				}
			}
		} catch (final Exception e) {
			System.out.println(e.getMessage());
			return StringUtils.EMPTY;
		}
	}

	public static final TestCondition ZK_IS_ON = new TestCondition() {
		@Override
		public boolean evaluate() {
			return !send4LetterWord(HOST, PORT, ZK_OK_CMD).isEmpty();
		}
	};

	public static final TestCondition ZK_IS_OFF = new TestCondition() {
		@Override
		public boolean evaluate() {
			return send4LetterWord(HOST, PORT, ZK_OK_CMD).isEmpty();
		}
	};
}
