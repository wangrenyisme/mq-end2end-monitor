package com.linkedin.xinfra.monitor.services.pulsar;

import com.linkedin.xinfra.monitor.common.DefaultTopicSchema;
import com.linkedin.xinfra.monitor.common.Utils;
import com.linkedin.xinfra.monitor.consumer.BaseConsumerRecord;
import com.linkedin.xinfra.monitor.consumer.KMBaseConsumer;
import com.linkedin.xinfra.monitor.consumer.PulsarConsumer;
import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.metrics.ConsumeMetrics;
import com.linkedin.xinfra.monitor.services.metrics.ProduceMetrics;
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
import java.util.concurrent.atomic.AtomicInteger;

/***/
public class PulsarConsumerService implements Service {
    private static final Logger log = LoggerFactory.getLogger(PulsarConsumerService.class);

    private final KMBaseConsumer consumer;
    private final ConsumeMetrics _sensors;
    private final String TAGS_NAME = "name";
    private final String _name;
    private final AtomicBoolean _running = new AtomicBoolean(false);
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);


    public PulsarConsumerService(Map<String, Object> props, String clusterName) {
        _name = clusterName;
        String topic = (String) props.get("topic");
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(props);
        consumer = new PulsarConsumer(topic, consumerProperties);
        MetricConfig metricConfig = new MetricConfig().samples(60).timeWindow(1000, TimeUnit.MILLISECONDS);
        List<MetricsReporter> reporters = new ArrayList<>();
        reporters.add(new JmxReporter(JMX_PREFIX));
        Metrics metrics = new Metrics(metricConfig, reporters, new SystemTime());
        Map<String, String> tags = new HashMap<>();
        tags.put(TAGS_NAME, _name);
        _sensors = new ConsumeMetrics(metrics, tags, 5000, 1);
    }

    @Override
    public void start() {
        _running.set(true);
        executorService.submit(new ConsumerHandler());
        log.info("{}/ProduceService started", _name);
    }

    @Override
    public void stop() {
        if (_running.compareAndSet(true, false)) {
            executorService.shutdown();
            consumer.close();
        }
    }

    @Override
    public boolean isRunning() {
        return _running.get();
    }

    @Override
    public void awaitShutdown(long timeout, TimeUnit unit) {
        executorService.shutdown();
    }

    class ConsumerHandler implements Runnable {
        @Override
        public void run() {
            while (_running.get()) {
                BaseConsumerRecord record = null;
                try {
                    record = consumer.receive();

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
                GenericRecord genericRecord = Utils.genericRecordFromJson(record.value());
                long prevMs = (long) genericRecord.get(DefaultTopicSchema.TIME_FIELD.name());
                long currMs = System.currentTimeMillis();
                consumer.commitAsync();
                _sensors._recordsDelay.record(currMs - prevMs);
                _sensors._recordsConsumed.record();
                _sensors._bytesConsumed.record(record.value().length());
            }
        }
    }
}
