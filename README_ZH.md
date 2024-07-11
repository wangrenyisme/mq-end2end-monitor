mq-end2end-monitor 项目 fork 自 Xinfra Monitor，支持 Kafka和Pulsar 的端到端延迟监控，支持多集群接入，接入 Prometheus JMX Exporter 方便您快速集成监控告警
## HTTP 监控
### 方案1: 采用 jmx_exporter 
打包镜像时带上，以Java agent 方式运行 
https://github.com/prometheus/jmx_exporter

```shell
java -javaagent:./jmx_prometheus_javaagent-1.0.1.jar=12345:config.yaml -jar yourJar.jar
```

### 方案2: Spring Boot 的 Actuator + Micrometer
结合使用 Spring Boot 的 Actuator 模块和 Micrometer 库，因为 Micrometer 提供了将 JMX 指标转换为 Prometheus 或其他监控系统可读格式的功能
## Pulsar 支持

