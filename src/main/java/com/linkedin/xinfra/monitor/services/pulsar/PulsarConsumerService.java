/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.services.pulsar;

import com.linkedin.xinfra.monitor.common.DefaultTopicSchema;
import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.consumer.BaseConsumerRecord;
import com.linkedin.xinfra.monitor.consumer.KMBaseConsumer;
import com.linkedin.xinfra.monitor.consumer.PulsarConsumer;
import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.metrics.ConsumeMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;

/***/
public class PulsarConsumerService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerService.class);

  private final KMBaseConsumer _consumer;
  private final ConsumeMetrics _sensors;
  private final String _name;
  private final AtomicBoolean _running = new AtomicBoolean(false);
  private final ExecutorService _executorService = Executors.newFixedThreadPool(1);


  public PulsarConsumerService(Map<String, Object> props, String clusterName) {
    String tagName = "name";
    _name = clusterName;
    String topic = (String) props.get("topic");
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(props);
    _consumer = new PulsarConsumer(topic, consumerProperties);
    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put(tagName, _name);
    _sensors = new ConsumeMetrics(metrics, tags, 5000, 1);
  }

  @Override
  public void start() {
    _running.set(true);
    _executorService.submit(new ConsumerHandler());
    LOG.info("{}/PulsarConsumerService started", _name);
  }

  @Override
  public void stop() {
    if (_running.compareAndSet(true, false)) {
      _executorService.shutdown();
      _consumer.close();
    }
  }

  @Override
  public boolean isRunning() {
    return _running.get();
  }

  @Override
  public void awaitShutdown(long timeout, TimeUnit unit) {
    _executorService.shutdown();
  }

  class ConsumerHandler implements Runnable {
    @Override
    public void run() {
      while (_running.get()) {
        BaseConsumerRecord record = null;
        try {
          record = _consumer.receive();

        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
        GenericRecord genericRecord = Utils.genericRecordFromJson(record.value());
        long prevMs = (long) genericRecord.get(DefaultTopicSchema.TIME_FIELD.name());
        long currMs = System.currentTimeMillis();
        _consumer.commitAsync();
        _sensors._recordsDelay.record(currMs - prevMs);
        _sensors._recordsConsumed.record();
        _sensors._bytesConsumed.record(record.value().length());
      }
    }
  }
}
