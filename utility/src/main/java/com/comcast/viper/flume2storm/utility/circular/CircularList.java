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
  /**
   * Adds an element to the list
   * 
   * @param toAdd
   *          The element to add
   * @return True if the element was successfully added to the list, false
   *         otherwise
   */
  boolean add(final T toAdd);

  /**
   * Removes an element from the list
   * 
   * @param toRemove
   *          The element to remove
   * @return True if the element was successfully removed to the list, false
   *         otherwise
   */
  boolean remove(final T toRemove);

  /**
   * @return True if the list is empty, false ottherwise
   */
  boolean isEmpty();

  /**
   * @return The number of (distinct) elements in the list
   */
  int size();

  /**
   * @return The next element, or null if the list is empty
   */
  T getNext();
}