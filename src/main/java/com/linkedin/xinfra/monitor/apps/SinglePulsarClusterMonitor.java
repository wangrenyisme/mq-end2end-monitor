package com.linkedin.xinfra.monitor.apps;

import com.linkedin.xinfra.monitor.services.Service;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarConsumerService;
import com.linkedin.xinfra.monitor.services.pulsar.PulsarProduceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/***/
public class SinglePulsarClusterMonitor implements App {
    private static final Logger LOG = LoggerFactory.getLogger(SinglePulsarClusterMonitor.class);
    private static final int SERVICES_INITIAL_CAPACITY = 4;
    private final List<Service> _allServices;
    private final String _clusterName;

    public SinglePulsarClusterMonitor(Map<String, Object> props, String clusterName) throws Exception {
        _allServices = new ArrayList<>(SERVICES_INITIAL_CAPACITY);
        _clusterName = clusterName;
        PulsarProduceService produceService = new PulsarProduceService(props, clusterName);
        PulsarConsumerService consumeService = new PulsarConsumerService(props,clusterName);
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
        return false;
    }

    @Override
    public void awaitShutdown() {

    }
}
