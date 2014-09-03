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

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import com.google.common.base.Preconditions;

/**
 * ZooKeeper node creation argument. See ZooKeeper's documentation for details
 * on every one of them, except {@link #createHierarchy} which creates the
 * parent of the nodes if missing
 */
public class ZkNodeCreationArg {
	public static final byte[] DEFAULT_DATA = new byte[0];
	public static final List<ACL> DEFAULT_ACL = Ids.OPEN_ACL_UNSAFE;
	public static final CreateMode DEFAULT_CREATE_MODE = CreateMode.PERSISTENT;
	public static final boolean DEFAULT_CREATE_HIERARCHY = true;

	protected byte[] data;
	protected List<ACL> acl;
	protected CreateMode createMode;
	protected boolean createHierarchy;

	public ZkNodeCreationArg() {
		data = DEFAULT_DATA;
		acl = DEFAULT_ACL;
		createMode = DEFAULT_CREATE_MODE;
		createHierarchy = DEFAULT_CREATE_HIERARCHY;
	}

	public ZkNodeCreationArg(final ZkNodeCreationArg other) {
		data = other.data;
		acl = other.acl;
		createMode = other.createMode;
		createHierarchy = other.createHierarchy;
	}

	public byte[] getData() {
		return data;
	}

	public ZkNodeCreationArg setData(final byte[] data) {
		Preconditions.checkNotNull(data, "What about an empty array, like it was before you tried to change it to null?");
		this.data = data;
		return this;
	}

	public List<ACL> getAcl() {
		return acl;
	}

	public ZkNodeCreationArg setAcl(final List<ACL> acl) {
		Preconditions.checkNotNull(acl, "No ACL specified. Wanna try Ids.OPEN_ACL_UNSAFE maybe?");
		this.acl = acl;
		return this;
	}

	public CreateMode getCreateMode() {
		return createMode;
	}

	public ZkNodeCreationArg setCreateMode(final CreateMode createMode) {
		Preconditions.checkNotNull(createMode, "Make up your mind!");
		this.createMode = createMode;
		return this;
	}

	public boolean isCreateHierarchy() {
		return createHierarchy;
	}

	public ZkNodeCreationArg setCreateHierarchy(final boolean createHierarchy) {
		this.createHierarchy = createHierarchy;
		return this;
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((acl == null) ? 0 : acl.hashCode());
		result = prime * result + (createHierarchy ? 1231 : 1237);
		result = prime * result + ((createMode == null) ? 0 : createMode.hashCode());
		result = prime * result + Arrays.hashCode(data);
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
		final ZkNodeCreationArg other = (ZkNodeCreationArg) obj;
		if (acl == null) {
			if (other.acl != null) {
				return false;
			}
		} else if (!acl.equals(other.acl)) {
			return false;
		}
		if (createHierarchy != other.createHierarchy) {
			return false;
		}
		if (createMode != other.createMode) {
			return false;
		}
		if (!Arrays.equals(data, other.data)) {
			return false;
		}
		return true;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "CreationArg [data=" + Arrays.toString(data) + ", acl=" + acl + ", createMode=" + createMode + ", createHierarchy=" + createHierarchy + "]";
	}
}
