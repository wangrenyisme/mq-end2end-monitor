/**
 * Copyright 2020 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.xinfra.monitor.apps;

import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarConsumerService;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarProduceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SinglePulsarClusterMonitor implements App {
  private static final Logger LOG = LoggerFactory.getLogger(SinglePulsarClusterMonitor.class);
  private final List<Service> _allServices;
  private final String _clusterName;

  public SinglePulsarClusterMonitor(Map<String, Object> props, String clusterName) throws Exception {
    _allServices = new ArrayList<>();
    _clusterName = clusterName;
    PulsarProduceService produceService = new PulsarProduceService(props, clusterName);
    PulsarConsumerService consumeService = new PulsarConsumerService(props, clusterName);
    _allServices.add(produceService);
    _allServices.add(consumeService);
  }

  @Override
  public void start() throws Exception {
    for (Service service : _allServices) {
      if (!service.isRunning()) {
        LOG.debug("Now starting {}", service.getServiceName());
        service.start();
      }
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
}
