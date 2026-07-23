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
import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import com.linkedin.xinfra.monitor.services.metrics.ConsumeMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/***/
public class PulsarConsumerService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarConsumerService.class);
  private final String _name;
  private final AtomicBoolean _running = new AtomicBoolean(false);
  private final ExecutorService _executorService = Executors.newFixedThreadPool(1);
  private final int _latencySlaMs;
  private final KMBaseConsumer _consumer;
  private final ConsumeMetrics _sensors;


  public PulsarConsumerService(Map<String, Object> props, String clusterName) throws Exception {
    String tagName = "name";
    _name = clusterName;
    String topic = (String) props.get("topic");
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(props);
    _latencySlaMs = (Integer) props.get(PulsarServiceConfig.CONSUME_LATENCY_SLA_MS);
    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put(tagName, _name);
    _sensors = new ConsumeMetrics(metrics, tags, 5000, 1);
    _consumer = new PulsarConsumer(topic, consumerProperties);
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
      Map<Integer, Long> nextIndexes = new HashMap<>();

      while (_running.get()) {
        BaseConsumerRecord record = null;
        try {
          record = _consumer.receive();
          if (record == null) {
            LOG.error("{}/PulsarConsumerService: receive() timed out, Pulsar broker may be unavailable.", _name);
            _sensors._consumeError.record();
            continue;
          }
          GenericRecord genericRecord = Utils.genericRecordFromJson(record.value());
          long prevMs = (long) genericRecord.get(DefaultTopicSchema.TIME_FIELD.name());
          long index = (long) genericRecord.get(DefaultTopicSchema.INDEX_FIELD.name());
          int partition = record.partition();
          long currMs = System.currentTimeMillis();
          _consumer.commitAsync();
          _sensors._recordsDelay.record(currMs - prevMs);
          _sensors._recordsConsumed.record();
          _sensors._bytesConsumed.record(record.value().length());
          if (currMs - prevMs > _latencySlaMs) _sensors._recordsDelayed.record();
          if (index == -1L || !nextIndexes.containsKey(partition)) {
            nextIndexes.put(partition, -1L);
            continue;
          }

          long nextIndex = nextIndexes.get(partition);

          if (nextIndex == -1 || index == nextIndex || index <= 1) {
            nextIndexes.put(partition, index + 1);
          } else if (index < nextIndex) {
              _sensors._recordsDuplicated.record();
          } else { // this will equate to the case where index > nextIndex...
            nextIndexes.put(partition, index + 1);
            long numLostRecords = index - nextIndex;
            _sensors._recordsLost.record(numLostRecords);
            LOG.info("_recordsLost recorded: Avro record current index: {} at timestamp {}. Next index: {}. Lost {} records.", index, currMs, nextIndex, numLostRecords);
          }
        } catch (Exception e) {
          _sensors._consumeError.record();
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }
}
