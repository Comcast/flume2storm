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
package com.comcast.viper.flume2storm.connection.receptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.comcast.viper.flume2storm.connection.parameters.SimpleConnectionParameters;
import com.comcast.viper.flume2storm.connection.receptor.EventReceptor;
import com.comcast.viper.flume2storm.connection.sender.EventSender;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSender;
import com.comcast.viper.flume2storm.connection.sender.SimpleEventSenderRouter;
import com.comcast.viper.flume2storm.event.F2SEvent;

/**
 */
public class SimpleEventReceptor implements EventReceptor<SimpleConnectionParameters> {
  protected final Queue<F2SEvent> events;
  protected final SimpleConnectionParameters connectionParameters;
  protected final AtomicBoolean started;
  protected final AtomicBoolean connected;
  protected Thread internalThread;

  private class MaintainConnection extends Thread {
    private SimpleEventReceptor simpleEventReceptor;

    public MaintainConnection(SimpleEventReceptor simpleEventReceptor) {
      this.simpleEventReceptor = simpleEventReceptor;
    }

    /**
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      while (started.get()) {
        try {
          // Try connecting
          if (!connected.get()) {
          }
          Thread.sleep(50);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      // Disconnecting
      if (connected.get()) {
        SimpleEventSender eventSender = SimpleEventSenderRouter.getInstance().get(connectionParameters);
        if (eventSender != null) {
          System.out.println("Disconnecting....");
          eventSender.disconnect(simpleEventReceptor);
          connected.set(false);
        }
      }
    }
  }

  public SimpleEventReceptor(SimpleConnectionParameters connectionParameters) {
    events = new LinkedList<F2SEvent>();
    this.connectionParameters = connectionParameters;
    started = new AtomicBoolean(false);
    connected = new AtomicBoolean(false);
    System.out.println("Got conn param: " + connectionParameters);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getConnectionParameters()
   */
  @Override
  public SimpleConnectionParameters getConnectionParameters() {
    return connectionParameters;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#start()
   */
  @Override
  public boolean start() {
    // if (started.get())
    // return false;
    // started.set(true);
    SimpleEventSender eventSender = SimpleEventSenderRouter.getInstance().get(connectionParameters);
    if (eventSender != null) {
      System.out.println("Connecting to " + eventSender);
      eventSender.connect(this);
      connected.set(true);
    }
    //
    // internalThread = new MaintainConnection(this);
    // internalThread.start();
    return true;
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#stop()
   */
  @Override
  public boolean stop() {
    SimpleEventSender eventSender = SimpleEventSenderRouter.getInstance().get(connectionParameters);
    if (eventSender != null) {
      System.out.println("Disconnecting from " + eventSender);
      eventSender.disconnect(this);
      connected.set(false);
    }
    return true;
    // if (!started.get())
    // return false;
    // started.set(false);
    // try {
    // internalThread.join();
    // return true;
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // return false;
    // }
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#isConnected()
   */
  @Override
  public boolean isConnected() {
    return connected.get();
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents()
   */
  @Override
  public List<F2SEvent> getEvents() {
    return getEvents(Integer.MAX_VALUE);
  }

  /**
   * @see com.comcast.viper.flume2storm.connection.receptor.EventReceptor#getEvents(int)
   */
  @Override
  public List<F2SEvent> getEvents(int maxEvents) {
    final List<F2SEvent> result = new ArrayList<F2SEvent>();
    for (int i = 0; i < maxEvents; i++) {
      final F2SEvent e = events.poll();
      if (e == null) {
        break;
      }
      result.add(e);
    }
    return result;
  }

  /**
   * Method called by the {@link EventSender} to simulate sending a Flume2Storm
   * event
   * 
   * @param event
   *          The event to receive
   */
  public void receive(F2SEvent event) {
    events.add(event);
  }
}