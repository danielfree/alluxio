/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.heartbeat;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * This class can be used for controlling heartbeat execution of threads.
 *
 * In particular, the {@link ScheduledTimer} blocks on the {@link #tick()} method, waiting for
 * someone to invoke the {@link #schedule()} method.
 *
 * The contract of this class is that the {@link #schedule()} method should only be called after the
 * {@link #tick()} was called and that there is exactly one {@link #schedule()} call per
 * {@link #tick()} call.
 *
 * The {@link #schedule()} method is not meant to be invoked directly. Instead, the
 * {@link HeartbeatScheduler} class should be used.
 */
@ThreadSafe
public final class ScheduledTimer implements HeartbeatTimer {
  private final String mThreadName;
  private final Lock mLock;
  private final Condition mCondition;
  /** True when schedule() has been called, but tick() hasn't finished. **/
  private volatile boolean mScheduled;

  /**
   * Creates a new instance of {@link ScheduledTimer}.
   *
   * @param threadName the thread name
   * @param intervalMs the heartbeat interval (unused)
   */
  public ScheduledTimer(String threadName, long intervalMs) {
    mThreadName = threadName;
    mLock = new ReentrantLock();
    mCondition = mLock.newCondition();
    mScheduled = false;
  }

  /**
   * @return the thread name
   */
  public String getThreadName() {
    return mThreadName;
  }

  /**
   * Schedules execution of the heartbeat.
   */
  protected void schedule() {
    mLock.lock();
    try {
      Preconditions.checkState(!mScheduled, "Called schedule twice without waiting for any ticks");
      mScheduled = true;
      mCondition.signal();
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Waits until the heartbeat is scheduled for execution.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  public synchronized void tick() throws InterruptedException {
    mLock.lock();
    try {
      HeartbeatScheduler.addTimer(this);
      // Wait in a loop to handle spurious wakeups
      while (!mScheduled) {
        mCondition.await();
      }
      mScheduled = false;
    } finally {
      mLock.unlock();
    }
  }
}