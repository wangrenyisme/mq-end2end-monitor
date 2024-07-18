mq-end2end-monitor 项目 fork 自 Xinfra Monitor，支持 Kafka 和 Pulsar 的端到端延迟监控， 支持同时配置多集群，提供了兼容 prometheus 的 HTTP 端口。

端到端延迟：在一个进程里实现了生产者和消费者，消息体里包含消息创建时间戳，消费者收到消息时计算时间差得到端到端延迟。

HTTP 端口：采用 jmx_prometheus，通过 javaagent 的方式，将 JMX 指标通过 HTTP的方式暴露，方便您快速与现有的监控告警系统集成

可监控的指标有
```shell
records-consumed-rate 消费速率
records-delay-ms-99th p99端到端延迟 
records-delay-ms-avg 平均端到端延迟
records-produced-rate 生产速率
produce-delay-ms-99th p99生产延迟
produce-delay-ms-avg 平均生产延迟
......
```

# 运行
### Build Xinfra Monitor
```
$ git clone https://github.com/wangrenyisme/mq-end2end-monitor.git
$ cd mq-end2end-monitor 
$ ./gradlew jar
```

### Start XinfraMonitor to run tests/services specified in the config file
```
$ cd tools
$ wget https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar
$ cd ..
$ ./bin/xinfra-monitor-start.sh config/xinfra-monitor-dev.json
```


# 配置
[xinfra-monitor-examples.json](config%2Fxinfra-monitor-examples.json) 文件中演示了 Kafka（开ACL&不开ACL） 及 Pulsar 的配置方式

# 设计
## HTTP 端口设计
需要一个支持 prometheus 采集规范的 HTTP 端口。
### 方案1: 采用 jmx_exporter 
打包镜像时带上，以Java agent 方式运行 
https://github.com/prometheus/jmx_exporter

```shell
java -javaagent:./jmx_prometheus_javaagent-1.0.1.jar=12345:config.yaml -jar yourJar.jar
```

### 方案2: Spring Boot 的 Actuator + Micrometer
结合使用 Spring Boot 的 Actuator 模块和 Micrometer 库，因为 Micrometer 提供了将 JMX 指标转换为 Prometheus 或其他监控系统可读格式的功能
