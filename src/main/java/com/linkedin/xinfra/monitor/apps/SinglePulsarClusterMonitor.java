/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.apps;

import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.configs.PulsarServiceConfig;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarConsumerService;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarProduceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SinglePulsarClusterMonitor implements App {
  private static final Logger LOG = LoggerFactory.getLogger(SinglePulsarClusterMonitor.class);
  private final List<Service> _allServices;
  private final String _clusterName;
  private final Map<String, Object> _props;
  private PulsarAdmin _pulsarAdmin;

  public SinglePulsarClusterMonitor(Map<String, Object> props, String clusterName) throws Exception {
    _allServices = new ArrayList<>();
    _clusterName = clusterName;
    this._props = props;
  }

  private boolean initResources() {
    try {
      PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().serviceHttpUrl((String) _props.get(PulsarServiceConfig.HTTP_URL));
      String authPlugin = (String) _props.get(PulsarServiceConfig.AUTH_PLUGIN);
      String authData = (String) _props.get(PulsarServiceConfig.AUTH_DATA);
      if (authPlugin != null && !authPlugin.isEmpty()) {
        adminBuilder.authentication(authPlugin, authData);
      }
      this._pulsarAdmin = adminBuilder.build();
      CompletableFuture<Integer> topicPartitionResult = getOrCreateTopic();
      PulsarProduceService produceService = new PulsarProduceService(_props, topicPartitionResult, _clusterName);
      PulsarConsumerService consumeService = new PulsarConsumerService(_props, _clusterName);
      _allServices.add(produceService);
      _allServices.add(consumeService);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to init resources,{}", _clusterName, e);
      try {
        _pulsarAdmin.close();
        _allServices.forEach(Service::stop);
      } catch (Exception e1) {
        LOG.error("Failed to close pulsar admin", e1);
      }
      _allServices.clear();
      return false;
    }
  }

  @Override
  public void start() throws Exception {
    boolean initResources = initResources();
    if (initResources) {
      for (Service service : _allServices) {
        if (!service.isRunning()) {
          LOG.debug("Now starting {}", service.getServiceName());
          service.start();
        }
      }
      LOG.info(_clusterName + "/SinglePulsarClusterMonitor started.");
    } else {
      //todo failed retry
      LOG.warn(_clusterName + "/SinglePulsarClusterMonitor start failed.");
    }
  }

  @Override
  public void stop() {
    for (Service service : _allServices) {
      service.stop();
    }
    LOG.info(_clusterName + "/SingleClusterMonitor stopped.");
  }

  @Override
  public boolean isRunning() {
    for (Service service : _allServices) {
      if (!service.isRunning()) {
        LOG.error("{} service is not running", service.getServiceName());
        return false;
      }
    }
    return true;
  }

  @Override
  public void awaitShutdown() {
    for (Service service : _allServices) {
      service.awaitShutdown(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
  }


  public CompletableFuture<Integer> getOrCreateTopic() {
    CompletableFuture<Integer> result = new CompletableFuture<>();
    try {
      String topic = (String) _props.get(PulsarServiceConfig.TOPIC);
      String[] parts = topic.split("/");
      if (parts.length < 3) {
        throw new IllegalArgumentException("Invalid topic format, expected persistent://tenant/namespace/topic");
      }
      String tenant = parts[2];
      String namespace = parts[2] + "/" + parts[3];
      int partitions = (Integer) _props.get(PulsarServiceConfig.PARTITIONS);
      boolean autoCreateTopic = Boolean.parseBoolean(_props.getOrDefault(PulsarServiceConfig.TOPIC_CREATION_ENABLED, "true").toString());

      try {
        int exists = checkIfExists(tenant, namespace, topic);
        if (exists > 0) {
          result.complete(exists);
        } else if (autoCreateTopic) {
          createNamespaceAndTopic(tenant, namespace, topic, partitions);
          result.complete(partitions);
        } else {
          result.completeExceptionally(new RuntimeException("Topic not exists and not creatable"));
        }
      } catch (Exception e) {
        result.completeExceptionally(e);
      }
    } catch (Exception e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  /************************************private********************************************************/

  private int checkIfExists(String tenant, String namespace, String topic) throws PulsarAdminException {
    if (_pulsarAdmin.tenants().getTenants().contains(tenant) && _pulsarAdmin.namespaces().getNamespaces(tenant).stream().anyMatch(ns -> ns.equals(namespace))) {
      try {
        PartitionedTopicMetadata partitionedTopicMetadata = _pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
        return partitionedTopicMetadata.partitions;
      } catch (Exception e) {
        // not exists
      }
    }
    return -1;
  }

  private void createNamespaceAndTopic(String tenant, String namespace, String topic, int partitions) throws Exception {
    if (!_pulsarAdmin.tenants().getTenants().contains(tenant)) {
      _pulsarAdmin.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(Collections.singleton(this._clusterName)).build());
    }
    _pulsarAdmin.namespaces().getNamespaces(tenant).stream().filter(ns -> ns.equals(namespace)).findAny().orElseGet(() -> {
      try {
        _pulsarAdmin.namespaces().createNamespace(namespace, BundlesData.builder().numBundles(partitions).build());
        LOG.info("Created namespace: {}", namespace);
        return namespace;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create namespace: " + namespace, e);
      }
    });
    if (!_pulsarAdmin.topics().getPartitionedTopicList(namespace).contains(topic)) {
      _pulsarAdmin.topics().createPartitionedTopic(topic, partitions);
      LOG.info("Created partitioned topic {} with {} partitions", topic, partitions);
    }
  }
}
