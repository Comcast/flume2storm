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

import org.junit.Assert;
import org.junit.Test;

public class CircularListTest {
  public void testCircularList(final CircularList<Integer> list) {
    Assert.assertNull(list.getNext());
    Assert.assertTrue(list.isEmpty());
    Assert.assertEquals(0, list.size());
    Assert.assertNull(list.getNext());
    Assert.assertTrue(list.isEmpty());
    list.add(1);
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    list.add(2);
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(2), list.getNext());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(2), list.getNext());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    list.add(3);
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(3, list.size());
    Assert.assertEquals(Integer.valueOf(2), list.getNext());
    Assert.assertEquals(Integer.valueOf(3), list.getNext());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(2), list.getNext());
    Assert.assertEquals(Integer.valueOf(3), list.getNext());
    list.remove(2);
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(Integer.valueOf(1), list.getNext());
    Assert.assertEquals(Integer.valueOf(3), list.getNext());
    list.remove(1);
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals(1, list.size());
    list.remove(3);
    Assert.assertTrue(list.isEmpty());
    Assert.assertEquals(0, list.size());
    Assert.assertNull(list.getNext());
    Assert.assertNull(list.getNext());
  }

  @Test
  public void testIt() {
    testCircularList(new ReadWriteCircularList<Integer>());
  }
}
