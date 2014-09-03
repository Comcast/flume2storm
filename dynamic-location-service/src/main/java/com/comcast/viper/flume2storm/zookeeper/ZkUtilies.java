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

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.common.PathUtils;

import com.google.common.base.Preconditions;

/**
 * Utilies for ZkClient
 */
public class ZkUtilies {
	public static final Character SEPARATOR_CHAR = '/';
	public static final String SEPARATOR = SEPARATOR_CHAR.toString();

	/**
	 * Builds a valid (guaranteed) ZNode path made of the components passed in
	 * parameter. This method handles the path separator between component, so
	 * it can be called with or without them.
	 * 
	 * @param components
	 *            A bunch of ZNode path elements. Some may be null.
	 * @return The concatenated path of all the elements
	 * @throws IllegalArgumentException
	 *             if the path is invalid (empty for example)
	 */
	public static String buildZkPath(final String... components) {
		Preconditions.checkArgument(components != null, "No path element specified");
		boolean isFirst = true;
		final StringBuilder result = new StringBuilder();
		for (int i = 0; i < components.length; i++) {
			if (StringUtils.isEmpty(components[i])) {
				continue;
			}
			assert components[i] != null;
			// Checking path separator
			if (isFirst) {
				// First element must start with /
				if (!components[i].startsWith(SEPARATOR)) {
					result.append(SEPARATOR);
				}
				result.append(components[i]);
			} else {
				if (!SEPARATOR_CHAR.equals(result.charAt(result.length() - 1)) && !components[i].startsWith(SEPARATOR)) {
					result.append(SEPARATOR);
					result.append(components[i]);
				} else if (SEPARATOR_CHAR.equals(result.charAt(result.length() - 1)) && components[i].startsWith(SEPARATOR)) {
					result.append(components[i].substring(1));
				} else {
					result.append(components[i]);
				}
			}
			isFirst = false;
		}
		final String path = result.toString();
		PathUtils.validatePath(path);
		return path;
	}
}
