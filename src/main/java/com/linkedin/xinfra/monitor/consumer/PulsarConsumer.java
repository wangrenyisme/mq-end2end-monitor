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
import org.apache.pulsar.client.api.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Wraps around the new consumer from Apache Kafka and implements the #KMBaseConsumer interface
 */
public class PulsarConsumer implements KMBaseConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumer.class);
  private final Consumer<String> _consumer;
  private final PulsarClient _client;
  private Message<String> _message;
  private final Map<String,Integer> partitionMap = new HashMap<>();


  public PulsarConsumer(String topic, Properties consumerProperties) throws Exception {
    LOG.info("{} is being instantiated in the constructor..", this.getClass().getSimpleName());
    ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(consumerProperties.getProperty(PulsarServiceConfig.SERVICE_URL));
    String authPlugin = consumerProperties.getProperty(PulsarServiceConfig.AUTH_PLUGIN);
    String authData = consumerProperties.getProperty(PulsarServiceConfig.AUTH_DATA);
    if (authPlugin != null && !authPlugin.isEmpty()) {
      clientBuilder.authentication(authPlugin, authData);
    }
    _client = clientBuilder.build();
    _consumer = _client.newConsumer(Schema.STRING).topic(topic)
        .subscriptionName(consumerProperties.getProperty(PulsarServiceConfig.SUBSCRIPTION_NAME))
        .subscriptionType(SubscriptionType.Failover).subscribe();
  }

  @Override
  public BaseConsumerRecord receive() throws Exception {
    try {
      _message = _consumer.receive(10, TimeUnit.SECONDS);
      if (_message == null) {
        return null;
      }
      return new BaseConsumerRecord(_message.getTopicName(), getPartitionIndex(_message.getTopicName()), 0, _message.getKey(), _message.getValue());
    } catch (PulsarClientException e) {
      throw new Exception(e);
    }
  }

  private Integer getPartitionIndex(String topicPartitionName){
    Integer i = partitionMap.get(topicPartitionName);
    if(i == null){
      String[] split = topicPartitionName.split("-");
      try{
        i = Integer.parseInt(split[split.length-1]);
      }catch (Exception e){
        //ignore
        i = 0;
      }
      partitionMap.put(topicPartitionName, i);
    }
    return i;
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
