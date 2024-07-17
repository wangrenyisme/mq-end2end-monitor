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

import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.producer.BaseProducerRecord;
import com.linkedin.xinfra.monitor.producer.KMBaseProducer;
import com.linkedin.xinfra.monitor.producer.PulsarProducer;
import com.linkedin.xinfra.monitor.services.AbstractService;
import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import com.linkedin.xinfra.monitor.services.metrics.ProduceMetrics;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PulsarProduceService extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarProduceService.class);
  private final String _name;
  private final ProduceMetrics _sensors;
  private final int _produceDelayMs = 1000;
  private final boolean _sync = true;
  private final AtomicBoolean _running;
  private final int _recordSize = 30;
  private final String _topic;
  private final String _producerId = "default";
  private final ScheduledExecutorService _produceExecutor;
  private final int _partitionNum;
  private final Properties _producerProps;
  private KMBaseProducer _producer;

  public PulsarProduceService(Map<String, Object> props, String name) throws Exception {
    super(10, Duration.ofMinutes(1));
    _name = name;
    _running = new AtomicBoolean(false);
    _topic = (String) props.get(PulsarServiceConfig.TOPIC);
    _producerProps = new Properties();
    _producerProps.putAll(props);
    _partitionNum = getTopicPartitionNums();
    initializeProducer();
    _produceExecutor = Executors.newScheduledThreadPool(5, new ProduceServiceThreadFactory());
    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags, 1, 5000, new AtomicInteger(0), false);
  }

  private int getTopicPartitionNums() throws PulsarClientException, PulsarAdminException {
    PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(_producerProps.getProperty(PulsarServiceConfig.HTTP_URL)).authentication(AuthenticationFactory.token(_producerProps.getProperty(PulsarServiceConfig.TOKEN)));
    PulsarAdmin pulsarAdmin = pulsarAdminBuilder.build();
    Topics topicsClient = pulsarAdmin.topics();
    PartitionedTopicMetadata partitionedTopicMetadata = topicsClient.getPartitionedTopicMetadata(_topic);
    pulsarAdmin.close();
    int partitions = partitionedTopicMetadata.partitions;
    LOG.debug("{}/get topic partitions.", partitions);
    return partitions;

  }

  private void initializeProducer() throws Exception {
    _producer = new PulsarProducer(_producerProps, _partitionNum);
    LOG.info("{}/ProduceService is initialized.", _name);
  }

  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      for (int i = 0; i < _partitionNum; i++) {
        _produceExecutor.scheduleWithFixedDelay(new ProduceRunnable(i, null), _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
      }
      LOG.info("{}/ProduceService started", _name);
    }
  }

  @Override
  public synchronized void stop() {
    if (_running.compareAndSet(true, false)) {
      _produceExecutor.shutdown();
      _producer.close();
      LOG.info("{}/ProduceService stopped.", _name);
    }
  }

  @Override
  public void awaitShutdown(long timeout, TimeUnit unit) {
    try {
      _produceExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted when waiting for {}/ProduceService to shutdown.", _name);
    }
    LOG.info("{}/ProduceService shutdown completed.", _name);
  }


  @Override
  public boolean isRunning() {
    return _running.get();
  }

  /**
   * This creates the records sent to the consumer.
   */
  private class ProduceRunnable implements Runnable {
    private final int _partition;
    private final String _key;

    ProduceRunnable(int partition, String key) {
      _partition = partition;
      _key = key;
    }

    public void run() {
      try {
        long currMs = System.currentTimeMillis();
        String message = Utils.jsonFromFields(_topic, 0, currMs, _producerId, _recordSize);
        BaseProducerRecord record = new BaseProducerRecord(_topic, _partition, _key, message);
//                LOG.info("send message: {}",message);
        RecordMetadata metadata = _producer.send(record, _sync);
        _sensors._produceDelay.record(System.currentTimeMillis() - currMs);
        _sensors._recordsProduced.record();
      } catch (Exception e) {
        _sensors._produceError.record();
        LOG.warn(_name + " failed to send message", e);
      }
    }
  }

  private class ProduceServiceThreadFactory implements ThreadFactory {

    private final AtomicInteger _threadId = new AtomicInteger();

    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-produce-service-" + _threadId.getAndIncrement());
    }
  }

  private class HandleNewPartitionsThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      return new Thread(r, _name + "-produce-service-new-partition-handler");
    }
  }

}
