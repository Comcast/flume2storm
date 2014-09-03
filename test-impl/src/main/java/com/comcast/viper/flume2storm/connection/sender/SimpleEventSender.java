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
package com.comcast.viper.flume2storm.connection.sender;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.SimpleEventReceptor;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.event.F2SEvent;
import com.comcast.viper.flume2storm.location.SimpleServiceProvider;
import com.comcast.viper.flume2storm.utility.circular.CircularList;
import com.comcast.viper.flume2storm.utility.circular.ReadWriteCircularList;

/**
 */
public class SimpleEventSender extends SimpleServiceProvider implements EventSender<SimpleConnectionParameters> {
  private static final long serialVersionUID = -6829876645658796790L;
  private static final Random random = new Random();
  protected final CircularList<SimpleEventReceptor> receptors;

  /**
   * @param connectionParameters
   *          See {@link #getConnectionParameters()}
   */
  public SimpleEventSender(SimpleConnectionParameters connectionParameters) {
    super(connectionParameters);
    receptors = new ReadWriteCircularList<SimpleEventReceptor>();
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#start()
   */
  @Override
  public boolean start() {
    SimpleEventSenderRouter.getInstance().add(this);
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#stop()
   */
  @Override
  public boolean stop() {
    SimpleEventSenderRouter.getInstance().remove(this);
    return true;
  }

  /**
   * Method called by the SimpleEventReceptor to connect the EventSender
   * 
   * @param receptor
   *          The receptor that is connecting
   */
  public void connect(SimpleEventReceptor receptor) {
    receptors.add(receptor);
  }

  /**
   * Method called by the SimpleEventReceptor to disconnect the EventSender
   * 
   * @param receptor
   *          The receptor that is connecting
   */
  public void disconnect(SimpleEventReceptor receptor) {
    receptors.remove(receptor);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#getNbReceptors()
   */
  @Override
  public int getNbReceptors() {
    return receptors.size();
  }

  /**
   * Load-balances the events to send between the receptor connected
   * 
   * @see com.comcast.viper.flume2storm.connection.sender.EventSender#send(java.util.List)
   */
  @Override
  public int send(List<F2SEvent> events) {
    int result = 0;
    for (F2SEvent f2sEvent : events) {
      SimpleEventReceptor receptor = receptors.getNext();
      if (receptor == null) {
        // No more receptor connected
        break;
      }
      receptor.receive(f2sEvent);
      result++;
    }
    return result;
  }

  @Override
  public String toString() {
    return "SimpleEventSender [getId()=" + getId() + ", getConnectionParameters()=" + getConnectionParameters() + "]";
  }
}