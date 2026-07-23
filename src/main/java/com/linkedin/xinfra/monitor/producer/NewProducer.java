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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/*
 * Wrap around the new producer from Apache Kafka and implement the #KMBaseProducer interface
 */
public class NewProducer implements KMBaseProducer {

  private final KafkaProducer<String, String> _producer;

  public NewProducer(Properties producerProps) {
    _producer = new KafkaProducer<>(producerProps);
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync) throws Exception {
    return send(baseRecord, sync, null);
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync, ProduceCallback callback) throws Exception {
    ProducerRecord<String, String> record =
      new ProducerRecord<>(baseRecord.topic(), baseRecord.partition(), baseRecord.key(), baseRecord.value());
    if (sync) {
      // Block for the broker ack; any failure surfaces here as an ExecutionException.
      Future<RecordMetadata> future = _producer.send(record);
      try {
        RecordMetadata metadata = future.get();
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
    // Asynchronous send: the callback runs on the producer's I/O thread and is the only place where
    // a failure completing after this method returns can be observed.
    _producer.send(record, (metadata, exception) -> {
      if (callback != null) {
        callback.onCompletion(exception);
      }
    });
    return null;
  }

  @Override
  public void close() {
    _producer.close();
  }

}
