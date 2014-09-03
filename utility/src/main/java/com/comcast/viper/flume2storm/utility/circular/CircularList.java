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

/**
 * A circular list. Elements can be added or removed like any regular list. In
 * order to retrieve the list item, call to the {@link #getNext()} method will
 * return the next element in the list. The list is circular, so the next item
 * after the last element is the first element.
 * 
 * @param <T>
 *          Type of the items in the list
 */
public interface CircularList<T> {
  void add(final T toAdd);

  void remove(final T toRemove);

  boolean isEmpty();

  int size();

  /**
   * @return The next element, or null if the list is empty
   */
  T getNext();
}