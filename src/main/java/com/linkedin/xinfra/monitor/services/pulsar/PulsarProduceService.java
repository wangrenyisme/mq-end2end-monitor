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

import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.producer.BaseProducerRecord;
import com.linkedin.xinfra.monitor.producer.KMBaseProducer;
import com.linkedin.xinfra.monitor.producer.PulsarProducerHandler;
import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import com.linkedin.xinfra.monitor.services.metrics.ProduceMetrics;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarProduceService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarProduceService.class);
  private final String _name;
  private final ProduceMetrics _sensors;
  private final int _produceDelayMs;
  private final AtomicBoolean _running;
  private final String _topic;
  private final ScheduledExecutorService _produceExecutor;
  /** This can be updated while running when new partitions are added to the monitor topic. */
  private final ConcurrentMap<Integer, AtomicLong> _nextIndexPerPartition;
  private final boolean _sync;
  private final Properties _producerProps;
  private final KMBaseProducer _producer;
  private final int _partitionNum;

  public PulsarProduceService(Map<String, Object> props, CompletableFuture<Integer> partitionNum, String name) throws Exception {
    _name = name;
    _running = new AtomicBoolean(false);
    _topic = (String) props.get(PulsarServiceConfig.TOPIC);
    _producerProps = new Properties();
    _producerProps.putAll(props);
    _produceDelayMs = Integer.parseInt(_producerProps.getProperty(PulsarServiceConfig.PRODUCE_RECORD_DELAY_MS, "1000"));
    _produceExecutor = Executors.newScheduledThreadPool(5, new ProduceServiceThreadFactory());
    _nextIndexPerPartition = new ConcurrentHashMap<>();
    _sync = (boolean) props.getOrDefault(PulsarServiceConfig.PRODUCE_SYNC_CONFIG, false);
    MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters = new ArrayList<>();
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
    Map<String, String> tags = new HashMap<>();
    tags.put("name", _name);
    _sensors = new ProduceMetrics(metrics, tags, 1, 5000, new AtomicInteger(0), false);
    try {
      this._partitionNum = partitionNum.get();
    } catch (Exception e) {
      throw new RuntimeException(e.getCause());
    }
    _producer = new PulsarProducerHandler(_producerProps, _partitionNum);
  }


  @Override
  public synchronized void start() {
    if (_running.compareAndSet(false, true)) {
      for (int i = 0; i < _partitionNum; i++) {
        _produceExecutor.scheduleWithFixedDelay(new ProduceRunnable(i, null), _produceDelayMs, _produceDelayMs, TimeUnit.MILLISECONDS);
      }
      LOG.info("{}/ProduceService started with produce rate {}ms", _name, _produceDelayMs);
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
        AtomicLong indexAdder = _nextIndexPerPartition.computeIfAbsent(_partition, k -> new AtomicLong(0));
        long index = indexAdder.incrementAndGet();
        long currMs = System.currentTimeMillis();
        int _recordSize = 30;
        String _producerId = "default";
        String message = Utils.jsonFromFields(_topic, index, currMs, _producerId, _recordSize);
        BaseProducerRecord record = new BaseProducerRecord(_topic, _partition, _key, message);
        _producer.send(record, _sync);
        _sensors._produceDelay.record(System.currentTimeMillis() - currMs);
        _sensors._recordsProduced.record();
        _sensors._produceErrorInLastSendPerPartition.put(_partition, false);
      } catch (Exception e) {
        _sensors._produceError.record();
        _sensors._produceErrorInLastSendPerPartition.put(_partition, true);
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
}