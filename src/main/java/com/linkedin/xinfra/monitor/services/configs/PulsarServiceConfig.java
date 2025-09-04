/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.xinfra.monitor.services.configs;

public class PulsarServiceConfig {
    // Basic connection config
    public static final String SERVICE_URL = "service.url";
    public static final String HTTP_URL = "http.url";
    public static final String TOPIC = "topic";
    public static final String SUBSCRIPTION_NAME = "subscription.name";

    // Authentication config
    public static final String AUTH_PLUGIN = "authentication.plugin";
    public static final String AUTH_DATA = "authentication.data";

    // Topic management config
    public static final String TOPIC_MANAGEMENT_ENABLED = "topic-management.topicManagementEnabled";
    public static final String TOPIC_CREATION_ENABLED = "topic-management.topicCreationEnabled";
    public static final String TOPIC_TYPE = "topic-management.topicType";
    public static final String PERSISTENCE_ENSEMBLE_SIZE = "topic-management.persistence.ensembleSize";
    public static final String PERSISTENCE_WRITE_QUORUM = "topic-management.persistence.writeQuorum";
    public static final String PERSISTENCE_ACK_QUORUM = "topic-management.persistence.ackQuorum";
    public static final String PARTITIONS = "topic-management.partitions";
    public static final String PARTITIONS_TO_BROKERS_RATIO = "topic-management.partitionsToBrokersRatio";

    // Producer config
    public static final String PRODUCE_RECORD_DELAY_MS = "produce.record.delay.ms";
    public static final String PRODUCE_CONSUMER_PROPS = "produce.producer.props";

    // Consumer config
    public static final String CONSUME_LATENCY_SLA_MS = "consume.latency.sla.ms";
    public static final String CONSUME_CONSUMER_PROPS = "consume.consumer.props";
}
