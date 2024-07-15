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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * Wraps around the new consumer from Apache Kafka and implements the #KMBaseConsumer interface
 */
public class PulsarConsumer implements KMBaseConsumer {

    private static final Logger log = LoggerFactory.getLogger(PulsarConsumer.class);
    private final Consumer<String> consumer;
    private final PulsarClient client;
    private Message<String> message;

    public PulsarConsumer(String topic, Properties consumerProperties) {
        try {
            log.info("{} is being instantiated in the constructor..", this.getClass().getSimpleName());
            client = PulsarClient.builder().serviceUrl(consumerProperties.getProperty("url")).authentication(AuthenticationFactory.token(consumerProperties.getProperty("token"))).build();
            consumer = client.newConsumer(Schema.STRING).topic(topic).subscriptionName(consumerProperties.getProperty("subscription.name")).subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BaseConsumerRecord receive() throws Exception {
        try{
            message = consumer.receive();
//            log.info("received message: {}",message.getValue());
            return new BaseConsumerRecord(message.getTopicName(), 0, 0, message.getKey(), message.getValue());
        } catch (PulsarClientException e) {
            throw new Exception(e);
        }
    }

    @Override
    public void commitAsync() {
        try {
            consumer.acknowledge(message);
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
            consumer.close();
            client.close();
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
