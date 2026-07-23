/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.producer;

/**
 * A producer-agnostic callback invoked when an asynchronous send completes.
 *
 * <p>This allows the service layer to be notified of failures that happen on the producer's I/O
 * thread when {@code sync == false}. Without it, exceptions raised after a non-blocking send are
 * swallowed and never reflected in metrics.
 */
public interface ProduceCallback {

  /**
   * Called when the send has completed, either successfully or with an error.
   *
   * @param exception the exception thrown during send, or {@code null} if the send succeeded.
   */
  void onCompletion(Exception exception);
}
