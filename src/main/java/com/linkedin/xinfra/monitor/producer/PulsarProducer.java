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
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.Properties;

public class PulsarProducer implements KMBaseProducer {

  private final PulsarClient _client;
  private final Producer<String>[] _producers;
  private final int _partitions;


  public PulsarProducer(Properties producerProps, int partitions) throws PulsarClientException {
    _producers = new Producer[partitions];
    _partitions = partitions;
    _client = PulsarClient.builder().serviceUrl(producerProps.getProperty(PulsarServiceConfig.SERVICE_URL)).authentication(AuthenticationFactory.token(producerProps.getProperty(PulsarServiceConfig.TOKEN))).build();
    String topic = producerProps.getProperty(PulsarServiceConfig.TOPIC);
    for (int partition = 0; partition < _partitions; partition++) {
      this._producers[partition] = _client.newProducer(Schema.STRING).topic(topic + "-partition-" + partition).create();
    }
  }

  @Override
  public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync) throws Exception {
    TypedMessageBuilder<String> message = _producers[baseRecord.partition()].newMessage().key(baseRecord.key()).value(baseRecord.value());
    MessageId messageId;
    if (sync) {
      messageId = message.send();
    } else {
      message.sendAsync();
      messageId = null;  // Assuming async send does not return MessageId immediately
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
