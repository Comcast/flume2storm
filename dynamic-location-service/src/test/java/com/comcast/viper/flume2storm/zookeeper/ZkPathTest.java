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

import junit.framework.Assert;

import org.junit.Test;

public class ZkPathTest {
	@Test
	public void test() {
		// Test them all (for 2 elements)
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("test", "test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("test", "/test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("test/", "test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("test/", "/test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("/test", "test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("/test", "/test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("/test/", "test1"));
		Assert.assertEquals("/test/test1", ZkUtilies.buildZkPath("/test/", "/test1"));

		// More elements
		Assert.assertEquals("/test/test1/test2/test3", ZkUtilies.buildZkPath("test", "test1", "test2", "test3"));
		Assert.assertEquals("/test/test1/test2/test3", ZkUtilies.buildZkPath("/test", "test1/", "/test2", "test3"));

		// Null resilient
		Assert.assertEquals("/test1/test2/test3", ZkUtilies.buildZkPath(null, "test1/", "/test2", "test3"));
		Assert.assertEquals("/test/test2/test3", ZkUtilies.buildZkPath("/test", null, "/test2", "test3"));

		// Empty component resilient
		Assert.assertEquals("/test1/test2/test3", ZkUtilies.buildZkPath("", "test1/", "/test2", "test3"));
		Assert.assertEquals("/test/test2/test3", ZkUtilies.buildZkPath("/test", "", "/test2", "test3"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid1() {
		// No empty path
		ZkUtilies.buildZkPath();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid2() {
		// No empty path
		ZkUtilies.buildZkPath("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid3() {
		// No empty path
		ZkUtilies.buildZkPath(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalid4() {
		// Path can't end with /
		ZkUtilies.buildZkPath("/test1", "test2/");
	}

}
