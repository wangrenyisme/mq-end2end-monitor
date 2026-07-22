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
import scala.Int;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarProduceService implements Service {
  private static final Logger LOG = LoggerFactory.getLogger(PulsarProduceService.class);
  private final String _name;
  private final ProduceMetrics _sensors;
  private final Integer _produceDelayMs;
  private final Integer _produceRatePerSec;
  private final AtomicBoolean _running;
  private final String _topic;
  private final ExecutorService _produceExecutor;
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
    _produceDelayMs = (Integer) _producerProps.getOrDefault(PulsarServiceConfig.PRODUCE_RECORD_DELAY_MS, 1000);
    _produceRatePerSec = _producerProps.get(PulsarServiceConfig.PRODUCE_RATE_PER_SEC) == null ?
        1000 / _produceDelayMs : (Integer) _producerProps.get(PulsarServiceConfig.PRODUCE_RATE_PER_SEC);
    _produceExecutor = Executors.newVirtualThreadPerTaskExecutor();
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
      // 每分区一条虚拟线程，各自以 _produceRatePerSec 条/秒的固定节奏发送。
      // 采用 LockSupport.parkNanos 而非阻塞式 RateLimiter：虚拟线程在 park 时会优雅 unmount 载体线程，
      // 零 pinning，真正契合虚拟线程模型。
      final long intervalNanos = _produceRatePerSec > 0 ? TimeUnit.SECONDS.toNanos(1) / _produceRatePerSec : 0L;
      for (int i = 0; i < _partitionNum; i++) {
        int partition = i;
        _produceExecutor.submit(() -> {
          // 以“下一节拍时间戳”驱动，避免每次发送耗时累积造成的速率漂移。
          long nextTickNanos = System.nanoTime();
          while (_running.get()) {
            new ProduceRunnable(partition, null).run();
            if (intervalNanos > 0) {
              nextTickNanos += intervalNanos;
              long sleepNanos = nextTickNanos - System.nanoTime();
              if (sleepNanos > 0) {
                LockSupport.parkNanos(sleepNanos);
              } else {
                // 已落后于目标节拍（如 send 耗时超过间隔），重置基准，不做补偿式追赶。
                nextTickNanos = System.nanoTime();
              }
            }
          }
        });
      }
      LOG.info("{}/ProduceService started with produce rate {} records/sec per partition ({} partitions)",
          _name, _produceRatePerSec, _partitionNum);
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