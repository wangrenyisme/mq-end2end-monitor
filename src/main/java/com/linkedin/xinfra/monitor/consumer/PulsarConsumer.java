/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.xinfra.monitor.consumer;

import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * Wraps around the new consumer from Apache Kafka and implements the #KMBaseConsumer interface
 */
public class PulsarConsumer implements KMBaseConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumer.class);
  private final Consumer<String> _consumer;
  private final PulsarClient _client;
  private Message<String> _message;

  public PulsarConsumer(String topic, Properties consumerProperties) {
    try {
      LOG.info("{} is being instantiated in the constructor..", this.getClass().getSimpleName());
      _client = PulsarClient.builder().serviceUrl(consumerProperties.getProperty(PulsarServiceConfig.SERVICE_URL)).authentication(AuthenticationFactory.token(consumerProperties.getProperty(PulsarServiceConfig.TOKEN))).build();
      _consumer = _client.newConsumer(Schema.STRING).topic(topic).subscriptionName(consumerProperties.getProperty(PulsarServiceConfig.SUBSCRIPTION_NAME)).subscriptionType(SubscriptionType.Exclusive).subscribe();
    } catch (PulsarClientException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public BaseConsumerRecord receive() throws Exception {
    try {
      _message = _consumer.receive();
      return new BaseConsumerRecord(_message.getTopicName(), 0, 0, _message.getKey(), _message.getValue());
    } catch (PulsarClientException e) {
      throw new Exception(e);
    }
  }

  @Override
  public void commitAsync() {
    try {
      _consumer.acknowledge(_message);
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition tp) {
    return null;
  }

  @Override
  public void close() {
    try {
      _consumer.close();
      _client.close();
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long lastCommitted() {
    return 0;
  }

  @Override
  public void updateLastCommit() {
  }
}
