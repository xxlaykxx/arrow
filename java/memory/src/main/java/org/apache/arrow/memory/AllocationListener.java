/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import io.netty.buffer.ArrowBuf;

/**
 * An allocation listener being notified for allocation/deallocation
 * <p>
 * It might be called from multiple threads if the allocator hierarchy shares a listener, in which
 * case, the provider should take care of making the implementation thread-safe.
 */
public interface AllocationListener {

  public static final AllocationListener NOOP = new AllocationListener() {
    @Override
    public void onAllocation(long size) {
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      return false;
    }

    @Override
    public boolean onRelease(long size) {
      return false;
    }

    @Override
    public boolean onKeepReleasedMemory(ArrowBuf arrowBuf) {
      throw new UnsupportedOperationException("NOOP listener will never keep released arrow buffer");
    }
  };

  /**
   * Called each time a new buffer is allocated
   *
   * @param size the buffer size being allocated
   */
  void onAllocation(long size);

  /**
   * Called whenever an allocation failed, giving the caller a chance to create some space in the allocator
   * (either by freeing some resource, or by changing the limit), and, if successful, allowing the allocator
   * to retry the allocation.
   *
   * @param size     the buffer size that was being allocated
   * @param outcome  the outcome of the failed allocation. Carries information of what failed
   * @return true, if the allocation can be retried; false if the allocation should fail
   */
  boolean onFailedAllocation(long size, AllocationOutcome outcome);

  /**
   * Called whenever a buffer is released and its refCount is down to zero, giving the caller a chance to keep
   * the memory in the buffer that is being released back to system.
   * @param size     the buffer size that is being released
   * @return true, if caller want to keep the memory in the buffer that is being released;
   *         false, otherwise and the memory will be released back to system
   */
  boolean onRelease(long size);

  /**
   * Called whenever a buffer is released and the caller want to keep the memory (return true in onRelease), giving
   * the caller a chance to keep the buffer with the memory that is going to be released.
   * @param arrowBuf     the buffer holding the memory that is going to be released
   * @return true, if caller want to keep the buffer; false, otherwise and the memory will be released back to system
   */
  boolean onKeepReleasedMemory(ArrowBuf arrowBuf);
}
