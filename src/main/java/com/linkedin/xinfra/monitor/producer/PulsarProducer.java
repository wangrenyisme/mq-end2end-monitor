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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pulsar.client.api.*;

import java.util.Arrays;
import java.util.Properties;

/*
 * Wrap around the new producer from Apache Kafka and implement the #KMBaseProducer interface
 */
public class PulsarProducer implements KMBaseProducer {

    private final PulsarClient client;
    private final Producer<String> producer;

    public PulsarProducer(Properties producerProps) throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(producerProps.getProperty("url")).authentication(AuthenticationFactory.token(producerProps.getProperty("token"))).build();
        producer = client.newProducer(Schema.STRING).topic(producerProps.getProperty("topic")).create();
    }

    @Override
    public RecordMetadata send(BaseProducerRecord baseRecord, boolean sync) throws Exception {
        TypedMessageBuilder<String> message = producer.newMessage().key(baseRecord.key()).value(baseRecord.value());
        if (sync) {
            MessageId messageId = message.send();
        } else {
            message.sendAsync();
        }
        return null;
    }

    @Override
    public void close() {
        try {
            producer.close();
            client.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

}
