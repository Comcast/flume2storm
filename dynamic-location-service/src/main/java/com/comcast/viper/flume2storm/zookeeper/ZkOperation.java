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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Preconditions;

/**
 * Wrapper around {@link ZooKeeper} and {@link ZkClient} to provide a safety net
 * and some convenience methods. For any operation, {@link #with(ZkClient)} and
 * {@link #path(String)} setters must be invoked, otherwise an exception will be
 * thrown
 */
@SuppressWarnings("javadoc")
public class ZkOperation {
  private ZkClient zkClient;
  private String path;

  //
  // Construction, setters, utilities
  //

  /**
   * @return A newly created {@link ZkOperation}
   */
  public static ZkOperation get() {
    return new ZkOperation();
  }

  public ZkOperation() {
  }

  public ZkOperation(final ZkClient zkClient, final String path) {
    this.zkClient = zkClient;
    this.path = path;
  }

  public ZkOperation with(final ZkClient zkClient) {
    this.zkClient = zkClient;
    return this;
  }

  public ZkOperation path(final String path) {
    this.path = path;
    return this;
  }

  protected String getNsPath() {
    Preconditions.checkNotNull(zkClient, "No ZkClient specified");
    Preconditions.checkNotNull(path, "No ZooKeeper paty specified");
    return ZkUtilies.buildZkPath(path);
  }

  protected ZooKeeper getZooKeeper() {
    Preconditions.checkState(zkClient.getState().isConnected(), "Not connected to ZooKeeper server");
    final ZooKeeper result = zkClient.getZooKeeper();
    Preconditions.checkState(result != null, "Failed to get ZooKeeper instance");
    return result;
  }

  //
  // Node management
  //

  /**
   * @see ZooKeeper#exists(String, boolean)
   * 
   * @return True if the specified ZNode exists, false otherwise
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public boolean nodeExists() throws KeeperException, InterruptedException {
    return getZooKeeper().exists(getNsPath(), false) != null;
  }

  /**
   * Creates a ZooKeeper node
   * 
   * @see ZooKeeper#create(String, byte[], List, CreateMode)
   * 
   * @param args
   *          Node creation arguments
   * @return The path of the node if created, or null otherwise
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public String createNode(final ZkNodeCreationArg args) throws KeeperException, InterruptedException {
    if (!args.isCreateHierarchy()) {
      return getZooKeeper().create(getNsPath(), args.getData(), args.getAcl(), args.getCreateMode());
    }
    final String[] pathElement = StringUtils.split(getNsPath(), ZkUtilies.SEPARATOR);
    Preconditions.checkArgument(pathElement.length > 0, "Invalid path: empty");
    String tmpPath = StringUtils.EMPTY;
    final ZkOperation zkOp = new ZkOperation().with(zkClient);
    // For all parent nodes:
    for (int i = 0; i < pathElement.length - 1; i++) {
      tmpPath = ZkUtilies.buildZkPath(tmpPath, pathElement[i]);
      if (!zkOp.path(tmpPath).nodeExists()) {
        zkOp.createNode(new ZkNodeCreationArg().setAcl(args.getAcl()));
      }
    }
    // Leaf node
    if (!nodeExists()) {
      return createNode(new ZkNodeCreationArg(args).setCreateHierarchy(false));
    }
    return path;
  }

  /**
   * @see ZooKeeper#delete(String, int)
   * 
   * @param version
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public void deleteNode(final int version) throws InterruptedException, KeeperException {
    Preconditions.checkArgument(version >= -1, "Zk node version needs to be positive or equals to -1");
    getZooKeeper().delete(getNsPath(), version);
  }

  /**
   * @see ZooKeeper#delete(String, int)
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public void deleteNode() throws InterruptedException, KeeperException {
    deleteNode(-1);
  }

  /**
   * Deletes the specified node and all its children nodes, if any
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public void deleteRecursive() throws InterruptedException, KeeperException {
    final List<String> children = getChildren();
    for (final String child : children) {
      new ZkOperation(zkClient, ZkUtilies.buildZkPath(path, child)).deleteRecursive();
    }
    deleteNode();
  }

  /**
   * @see ZooKeeper#getChildren(String, Watcher)
   * 
   * @param watcher
   *          May be null
   * @return The list of children of the specified path
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public List<String> getChildren(final Watcher watcher) throws KeeperException, InterruptedException {
    if (watcher == null) {
      return getZooKeeper().getChildren(getNsPath(), false);
    }
    return getZooKeeper().getChildren(getNsPath(), watcher);
  }

  /**
   * @see ZooKeeper#getChildren(String, Watcher)
   * 
   * @return The list of children of the specified path
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public List<String> getChildren() throws KeeperException, InterruptedException {
    return getChildren(null);
  }

  /**
   * @return A list of the specified node and all its children nodes
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public List<String> getChildrenRecursiveList() throws InterruptedException, KeeperException {
    final List<String> result = new LinkedList<String>();
    result.add(path);
    for (final String child : getChildren()) {
      result.addAll(new ZkOperation(zkClient, ZkUtilies.buildZkPath(path, child)).getChildrenRecursiveList());
    }
    return result;
  }

  /**
   * @see ZooKeeper#getData(String, Watcher, org.apache.zookeeper.data.Stat)
   * 
   * @param watcher
   *          May be null
   * @param stat
   * @return The contained by the node of the specified path
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public byte[] getData(final Watcher watcher, final Stat stat) throws KeeperException, InterruptedException {
    if (watcher == null) {
      return getZooKeeper().getData(getNsPath(), false, stat);
    }
    return getZooKeeper().getData(getNsPath(), watcher, stat);
  }

  /**
   * @see ZooKeeper#getData(String, Watcher, org.apache.zookeeper.data.Stat)
   *      (String, Watcher)
   * 
   * @return The contained by the node of the specified path
   * 
   * @throws KeeperException
   *           If a ZooKeeper exception occurred
   * @throws InterruptedException
   *           If the synchronous operation was interrupted
   * @throws IllegalStateException
   *           If not connected to the ZooKeeper server
   * @throws IllegalArgumentException
   *           If the arguments provided to this operation are invalid
   */
  public byte[] getData() throws KeeperException, InterruptedException {
    return getData(null, new Stat());
  }
}
