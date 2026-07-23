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

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A base producer used to abstract different producer classes.
 *
 * Implementations of this class must have constructor with the following signature:
 *   Constructor(java.util.Properties properties).
 */
public interface KMBaseProducer {

  RecordMetadata send(BaseProducerRecord record, boolean sync) throws Exception;

  /**
   * Send a record, notifying {@code callback} when the send completes.
   *
   * <p>For asynchronous sends ({@code sync == false}) the callback is the only way for the caller
   * to observe failures that occur after this method returns (e.g. on the producer's I/O thread).
   * The default implementation preserves the legacy behaviour by delegating to
   * {@link #send(BaseProducerRecord, boolean)} and invoking the callback with any exception thrown
   * synchronously; implementations should override it to also report asynchronous failures.
   */
  default RecordMetadata send(BaseProducerRecord record, boolean sync, ProduceCallback callback) throws Exception {
    try {
      RecordMetadata metadata = send(record, sync);
      if (callback != null) {
        callback.onCompletion(null);
      }
      return metadata;
    } catch (Exception e) {
      if (callback != null) {
        callback.onCompletion(e);
      }
      throw e;
    }
  }

  void close();

}
