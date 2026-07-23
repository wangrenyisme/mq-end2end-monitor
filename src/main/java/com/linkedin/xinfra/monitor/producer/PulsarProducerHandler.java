/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.producer;

import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PulsarProducerHandler implements KMBaseProducer {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarProducerHandler.class);
  private final PulsarClient _client;
  private final Producer<String>[] _producers;
  private final int _partitions;


  public PulsarProducerHandler(Properties producerProps, int partitions) throws Exception {
    _producers = new Producer[partitions];
    _partitions = partitions;

    String authPlugin = producerProps.getProperty(PulsarServiceConfig.AUTH_PLUGIN);
    String authData = producerProps.getProperty(PulsarServiceConfig.AUTH_DATA);
    Map produceProps = (Map) producerProps.get(PulsarServiceConfig.PRODUCE_CONSUMER_PROPS);
    boolean enableBatch = (boolean) produceProps.getOrDefault("enableBatch", false);
    int batchingMaxPublishDelayMs = (int) produceProps.getOrDefault("batchingMaxPublishDelayMs", 2);
    int batchingMaxBytes = (int) produceProps.getOrDefault("batchingMaxBytes", 102400);
    boolean blockIfQueueFull = (boolean) produceProps.getOrDefault("blockIfQueueFull", false);
    int maxPendingMessages = (int) produceProps.getOrDefault("maxPendingMessages", 10);
    int sendTimeoutMs = (int) produceProps.getOrDefault("sendTimeoutMs", 500);
    ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(producerProps.getProperty(PulsarServiceConfig.SERVICE_URL));
    if (authPlugin != null && !authPlugin.isEmpty()) {
      clientBuilder.authentication(authPlugin, authData);
    }
    _client = clientBuilder.build();
    String topic = producerProps.getProperty(PulsarServiceConfig.TOPIC);
    for (int partition = 0; partition < _partitions; partition++) {
      this._producers[partition] = _client.newProducer(Schema.STRING).enableBatching(enableBatch)
          .batchingMaxPublishDelay(batchingMaxPublishDelayMs, TimeUnit.MILLISECONDS)
          .batchingMaxBytes(batchingMaxBytes).maxPendingMessages(maxPendingMessages)
          .blockIfQueueFull(blockIfQueueFull).sendTimeout(sendTimeoutMs, TimeUnit.MILLISECONDS)
          .accessMode(ProducerAccessMode.WaitForExclusive).topic(topic + "-partition-" + partition).create();
    }
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync) throws Exception {
    return send(baseRecord, sync, null);
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync, ProduceCallback callback) throws Exception {
    TypedMessageBuilder<String> message = _producers[baseRecord.partition()].newMessage().key(baseRecord.key()).value(baseRecord.value());
    if (sync) {
      try {
        message.send();
        if (callback != null) {
          callback.onCompletion(null);
        }
      } catch (Exception e) {
        if (callback != null) {
          callback.onCompletion(e);
        }
        throw e;
      }
    } else {
      // Asynchronous send: report the outcome through the callback instead of throwing on the
      // internal Pulsar thread, where the exception would otherwise be swallowed.
      message.sendAsync().whenComplete((messageId, throwable) -> {
        if (throwable == null) {
          if (callback != null) {
            callback.onCompletion(null);
          }
          return;
        }
        // sendAsync() always completes exceptionally with a PulsarClientException (an Exception);
        // the Throwable->Exception adaptation exists only to satisfy the ProduceCallback signature
        // and to guard against a raw Error surfacing here.
        Exception ex = (throwable instanceof Exception) ? (Exception) throwable : new RuntimeException(throwable);
        if (callback != null) {
          callback.onCompletion(ex);
        } else {
          LOG.error("Async send failed and no callback was registered to report it", ex);
        }
      });
    }
    return null;
  }

  @Override
  public void close() {
    try {
      for (Producer<String> producer : _producers) {
        producer.close();
      }
      _client.close();
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

}
