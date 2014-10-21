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
package com.comcast.viper.flume2storm.utility.circular;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Implementation of CircularList that synchronizes access using a read-write
 * lock in order to achieve thread-safety.
 * 
 * @param <T>
 *          Type of the items in the list
 */
public class ReadWriteCircularList<T> implements CircularList<T> {
  protected final List<T> list;
  protected final transient ReadWriteLock lock;
  protected int pointer;

  /**
   * Constructor for a new empty circular list
   */
  public ReadWriteCircularList() {
    this(new ArrayList<T>());
  }

  /**
   * Constructor for a new circular list backed by the list provided
   * 
   * @param list
   *          A list of items
   */
  public ReadWriteCircularList(final List<T> list) {
    this.list = list;
    lock = new ReentrantReadWriteLock();
    pointer = 0;
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.circular.CircularList#isEmpty()
   */
  public boolean isEmpty() {
    try {
      lock.readLock().lock();
      return list.isEmpty();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.circular.CircularList#size()
   */
  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return list.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.circular.CircularList#add(java.lang.Object)
   */
  public boolean add(final T toAdd) {
    try {
      lock.writeLock().lock();
      return list.add(toAdd);
      // Programming note: no problem with pointer here
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.circular.CircularList#remove(java.lang.Object)
   */
  public boolean remove(final T toRemove) {
    try {
      lock.writeLock().lock();
      boolean result = list.remove(toRemove);
      if (pointer >= list.size()) {
        pointer = 0;
      }
      return result;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * @see com.comcast.viper.flume2storm.utility.circular.CircularList#getNext()
   */
  public T getNext() {
    try {
      lock.readLock().lock();
      if (list.isEmpty()) {
        return null;
      }
      final T result = list.get(pointer);
      pointer++;
      if (pointer >= list.size()) {
        pointer = 0;
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(list).append(pointer).hashCode();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ReadWriteCircularList<?> other = (ReadWriteCircularList<?>) obj;
    return new EqualsBuilder().append(this.list, other.list).append(this.pointer, other.pointer).isEquals();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ReadWriteCircularList [list=" + list + ", pointer=" + pointer + "]";
  }
}
