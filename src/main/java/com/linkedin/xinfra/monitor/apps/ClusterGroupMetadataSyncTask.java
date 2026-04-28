package com.linkedin.xinfra.monitor.apps;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Cluster-federation metadata sync task.
 *
 * <p>Periodically synchronises topic metadata (partition count, dynamic topic configs) and,
 * when every cluster in a group has ACL enabled, also synchronises Kafka ACL bindings.
 * Sync is bidirectional: the desired state is derived from the union of all clusters in a
 * group and then applied back to every cluster.
 *
 * <p>Config shape (inside the top-level monitor JSON):
 * <pre>
 * {
 *   "class.name": "com.linkedin.xinfra.monitor.apps.ClusterGroupMetadataSyncTask",
 *   "cluster-groups": {
 *     "my-group": {
 *       "cluster-a": { "bootstrap.servers": "...", ... },
 *       "cluster-b": { "bootstrap.servers": "...", ... }
 *     }
 *   },
 *   "sync.interval.seconds": 60,
 *   "excluded.topics": "__consumer_offsets,__transaction_state",
 *   "max.replication.factor": 3,
 *   "request.timeout.ms": 30000
 * }
 * </pre>
 */
public class ClusterGroupMetadataSyncTask implements App {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterGroupMetadataSyncTask.class);

    // ---- config keys ----
    private static final String CLUSTER_GROUPS_KEY = "cluster-groups";
    private static final String SYNC_INTERVAL_SECONDS = "sync.interval.seconds";
    private static final String EXCLUDED_TOPICS_KEY = "excluded.topics";
    private static final String MAX_REPLICATION_FACTOR_KEY = "max.replication.factor";
    private static final String REQUEST_TIMEOUT_MS_KEY = "request.timeout.ms";

    // ---- defaults ----
    private static final long DEFAULT_SYNC_INTERVAL_SECONDS = 60L;
    private static final short DEFAULT_MAX_REPLICATION_FACTOR = 3;
    private static final long DEFAULT_REQUEST_TIMEOUT_MS = 30_000L;
    private static final List<String> DEFAULT_EXCLUDED_TOPICS = Arrays.asList("__consumer_offsets",
        "__transaction_state", "__auto_balancer_metrics");

    // ---- instance fields ----
    private final String name;
    /**
     * groupName → (clusterName → adminClientProps)
     */
    private final Map<String, Map<String, Properties>> clusterGroups;
    private final long syncIntervalSeconds;
    private final short maxReplicationFactor;
    private final long requestTimeoutMs;
    private final Set<String> excludedTopics;

    private volatile boolean running = false;
    private ScheduledExecutorService scheduler;

    @SuppressWarnings("unchecked")
    public ClusterGroupMetadataSyncTask(String name, Map<String, Object> props) {
        this.name = name;

        Object groupsObj = props.get(CLUSTER_GROUPS_KEY);
        if (!(groupsObj instanceof Map)) {
            throw new IllegalArgumentException("'" + CLUSTER_GROUPS_KEY + "' must be a JSON object mapping group " +
                "names to cluster configs");
        }
        Map<String, Object> rawGroups = (Map<String, Object>) groupsObj;
        this.clusterGroups = parseClusterGroups(rawGroups);

        this.syncIntervalSeconds = parseLong(props.get(SYNC_INTERVAL_SECONDS), DEFAULT_SYNC_INTERVAL_SECONDS);
        this.maxReplicationFactor = (short) parseLong(props.get(MAX_REPLICATION_FACTOR_KEY),
            DEFAULT_MAX_REPLICATION_FACTOR);
        this.requestTimeoutMs = parseLong(props.get(REQUEST_TIMEOUT_MS_KEY), DEFAULT_REQUEST_TIMEOUT_MS);

        String excludedRaw = props.getOrDefault(EXCLUDED_TOPICS_KEY, "").toString().trim();
        if (excludedRaw.isEmpty()) {
            this.excludedTopics = new HashSet<>(DEFAULT_EXCLUDED_TOPICS);
        } else {
            this.excludedTopics =
                Arrays.stream(excludedRaw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
        }
    }

    // -------------------------------------------------------------------------
    // Constructor — called reflectively by the monitor loader
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // ── cluster-group01: mixed (one ACL cluster + one plain) ─────────────
        Map<String, Object> mainCluster = new HashMap<>();
        mainCluster.put("bootstrap.servers", "localhost:9092");
//        mainCluster.put("security.protocol", "SASL_PLAINTEXT");
//        mainCluster.put("sasl.mechanism", "SCRAM-SHA-256");
//        mainCluster.put("sasl.jaas.config",
//            "org.apache.kafka.common.security.scram.ScramLoginModule required"
//                + " username='your_username' password='your_password';");

        Map<String, Object> backupCluster = new HashMap<>();
        backupCluster.put("bootstrap.servers", "localhost:9093");

        Map<String, Object> group01 = new LinkedHashMap<>();
        group01.put("main-cluster", mainCluster);
        group01.put("backup-cluster", backupCluster);
//
//        // ── cluster-group02: both clusters have ACL — topics AND ACLs synced ─
//        Map<String, Object> primaryCluster = new HashMap<>();
//        primaryCluster.put("bootstrap.servers", "primary-host:9092");
//        primaryCluster.put("security.protocol", "SASL_PLAINTEXT");
//        primaryCluster.put("sasl.mechanism", "SCRAM-SHA-256");
//        primaryCluster.put("sasl.jaas.config",
//            "org.apache.kafka.common.security.scram.ScramLoginModule required"
//                + " username='admin' password='admin-secret';");
//
//        Map<String, Object> standbyCluster = new HashMap<>();
//        standbyCluster.put("bootstrap.servers", "standby-host:9092");
//        standbyCluster.put("security.protocol", "SASL_PLAINTEXT");
//        standbyCluster.put("sasl.mechanism", "SCRAM-SHA-256");
//        standbyCluster.put("sasl.jaas.config",
//            "org.apache.kafka.common.security.scram.ScramLoginModule required"
//                + " username='admin' password='admin-secret';");
//
//        Map<String, Object> group02 = new LinkedHashMap<>();
//        group02.put("primary-cluster", primaryCluster);
//        group02.put("standby-cluster", standbyCluster);
//
//        // ── assemble cluster-groups ──────────────────────────────────────────
        Map<String, Object> clusterGroups = new LinkedHashMap<>();
        clusterGroups.put("cluster-group01", group01);
//        clusterGroups.put("cluster-group02", group02);

        // ── top-level task props ─────────────────────────────────────────────
        Map<String, Object> props = new HashMap<>();
        props.put("cluster-groups", clusterGroups);
        props.put("sync.interval.seconds", 60);
        props.put("max.replication.factor", 3);
        props.put("request.timeout.ms", 30000);
        props.put("excluded.topics", "__consumer_offsets,__transaction_state");

        ClusterGroupMetadataSyncTask task = new ClusterGroupMetadataSyncTask("local-test", props);
        task.running = true;
        task.runSyncSafe();
    }

    // -------------------------------------------------------------------------
    // App lifecycle
    // -------------------------------------------------------------------------

    private static long parseLong(Object value, long defaultValue) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong(((String) value).trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Properties>> parseClusterGroups(Map<String, Object> rawGroups) {
        Map<String, Map<String, Properties>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> groupEntry : rawGroups.entrySet()) {
            String groupName = groupEntry.getKey();
            if (!(groupEntry.getValue() instanceof Map)) {
                throw new IllegalArgumentException("Cluster group '" + groupName + "' must be a JSON object");
            }
            Map<String, Object> rawClusters = (Map<String, Object>) groupEntry.getValue();
            if (rawClusters.size() < 2) {
                LOG.warn("Cluster group '{}' has fewer than 2 clusters — it will be skipped during sync.", groupName);
            }
            Map<String, Properties> clusters = new LinkedHashMap<>();
            for (Map.Entry<String, Object> clusterEntry : rawClusters.entrySet()) {
                String clusterName = clusterEntry.getKey();
                if (!(clusterEntry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("Cluster '" + clusterName + "' in group '" + groupName + "' " + "must be a JSON object");
                }
                Properties props = new Properties();
                props.putAll((Map<?, ?>) clusterEntry.getValue());
                clusters.put(clusterName, props);
            }
            result.put(groupName, clusters);
        }
        return result;
    }

    @Override
    public void start() {
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cluster-group-metadata-sync-" + name);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::runSyncSafe, 0, syncIntervalSeconds, TimeUnit.SECONDS);
        LOG.info("[{}] ClusterGroupMetadataSyncTask started — interval={}s, groups={}", name, syncIntervalSeconds,
            clusterGroups.keySet());
    }

    @Override
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("[{}] ClusterGroupMetadataSyncTask stopped.", name);
    }

    // -------------------------------------------------------------------------
    // Sync orchestration
    // -------------------------------------------------------------------------

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void awaitShutdown() {
        if (scheduler != null) {
            try {
                scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // -------------------------------------------------------------------------
    // State collection
    // -------------------------------------------------------------------------

    private void runSyncSafe() {
        if (!running) {
            return;
        }
        LOG.info("[{}] Starting metadata sync round for {} group(s)", name, clusterGroups.size());
        for (Map.Entry<String, Map<String, Properties>> groupEntry : clusterGroups.entrySet()) {
            try {
                syncGroup(groupEntry.getKey(), groupEntry.getValue());
            } catch (Exception e) {
                LOG.error("[{}] Unhandled error while syncing group '{}'", name, groupEntry.getKey(), e);
            }
        }
        LOG.info("[{}] Metadata sync round completed.", name);
    }

    // -------------------------------------------------------------------------
    // Applying desired state
    // -------------------------------------------------------------------------

    /**
     * Full sync for one cluster group.
     * All AdminClients are created at the start and closed in the finally block.
     */
    private void syncGroup(String groupName, Map<String, Properties> clusters) throws Exception {
        LOG.info("[{}][{}] Syncing {} clusters", name, groupName, clusters.size());

        Map<String, AdminClient> adminClients = new LinkedHashMap<>();
        try {
            for (Map.Entry<String, Properties> e : clusters.entrySet()) {
                Properties p = buildAdminProps(e.getValue());
                adminClients.put(e.getKey(), AdminClient.create(p));
            }

            boolean allAclEnabled = clusters.values().stream().allMatch(this::isAclEnabled);

            // Step 1 — collect the current state from every cluster
            // topicName → clusterName → TopicDescription
            Map<String, Map<String, TopicDescription>> allDescriptions = new HashMap<>();
            // topicName → clusterName → (configKey → configValue)  [dynamic configs only]
            Map<String, Map<String, Map<String, String>>> allConfigs = new HashMap<>();

            for (Map.Entry<String, AdminClient> ce : adminClients.entrySet()) {
                String clusterName = ce.getKey();
                AdminClient client = ce.getValue();
                collectClusterState(groupName, clusterName, client, allDescriptions, allConfigs);
            }

            if (allDescriptions.isEmpty()) {
                LOG.info("[{}][{}] No user topics found across any cluster, nothing to sync.", name, groupName);
                return;
            }

            // Step 2 — compute the desired target state (union / max)
            // topicName → desired partition count
            Map<String, Integer> targetPartitionCount = new HashMap<>();
            // topicName → desired dynamic configs (merged, first-seen wins on conflict)
            Map<String, Map<String, String>> targetTopicConfigs = new HashMap<>();
            // topicName → TopicDescription that represents the canonical source for RF etc.
            Map<String, TopicDescription> canonicalDescription = new HashMap<>();

            for (String topicName : allDescriptions.keySet()) {
                Map<String, TopicDescription> byCluster = allDescriptions.get(topicName);

                int maxPartitions = 0;
                TopicDescription canonical = null;
                for (Map.Entry<String, TopicDescription> e : byCluster.entrySet()) {
                    int cnt = e.getValue().partitions().size();
                    if (cnt > maxPartitions) {
                        maxPartitions = cnt;
                        canonical = e.getValue();
                    }
                }
                targetPartitionCount.put(topicName, maxPartitions);
                canonicalDescription.put(topicName, canonical);

                // Merge dynamic configs — first-seen wins when values differ
                Map<String, String> merged = new HashMap<>();
                Map<String, Map<String, String>> configsByCluster = allConfigs.getOrDefault(topicName,
                    Collections.emptyMap());
                for (Map.Entry<String, Map<String, String>> clusterConf : configsByCluster.entrySet()) {
                    for (Map.Entry<String, String> kv : clusterConf.getValue().entrySet()) {
                        String existing = merged.get(kv.getKey());
                        if (existing == null) {
                            merged.put(kv.getKey(), kv.getValue());
                        } else if (!existing.equals(kv.getValue())) {
                            LOG.warn("[{}][{}] Topic '{}' config '{}' conflict: '{}' (kept) vs '{}' (cluster: {})",
                                name, groupName, topicName, kv.getKey(), existing, kv.getValue(), clusterConf.getKey());
                        }
                    }
                }
                targetTopicConfigs.put(topicName, merged);
            }

            // Step 3 — apply desired state to each cluster
            for (Map.Entry<String, AdminClient> ce : adminClients.entrySet()) {
                String clusterName = ce.getKey();
                AdminClient client = ce.getValue();
                applyDesiredState(groupName, clusterName, client, targetPartitionCount, targetTopicConfigs,
                    canonicalDescription, clusters.get(clusterName));
            }

            // Step 4 — sync ACLs when every cluster has ACL enabled
            if (allAclEnabled) {
                syncAcls(groupName, adminClients);
            } else {
                LOG.info("[{}][{}] Not all clusters have ACL enabled — skipping ACL sync.", name, groupName);
            }

        } finally {
            for (AdminClient client : adminClients.values()) {
                closeQuietly(client);
            }
        }
    }

    private void collectClusterState(String groupName, String clusterName, AdminClient client, Map<String, Map<String
        , TopicDescription>> allDescriptions, Map<String, Map<String, Map<String, String>>> allConfigs) {
        try {
            Set<String> rawTopics = client.listTopics().names().get();
            Set<String> userTopics = rawTopics.stream().filter(t -> !isExcluded(t)).collect(Collectors.toSet());

            if (userTopics.isEmpty()) {
                LOG.info("[{}][{}] No user topics found.", name, clusterName);
                return;
            }

            // Descriptions
            Map<String, TopicDescription> descriptions = client.describeTopics(userTopics).all().get();
            for (Map.Entry<String, TopicDescription> e : descriptions.entrySet()) {
                allDescriptions.computeIfAbsent(e.getKey(), k -> new LinkedHashMap<>()).put(clusterName, e.getValue());
            }

            // Dynamic configs
            List<ConfigResource> resources =
                userTopics.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).collect(Collectors.toList());
            Map<ConfigResource, Config> configs = client.describeConfigs(resources).all().get();
            for (Map.Entry<ConfigResource, Config> e : configs.entrySet()) {
                String topicName = e.getKey().name();
                Map<String, String> dynamic = new HashMap<>();
                for (ConfigEntry ce : e.getValue().entries()) {
                    if (ce.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        dynamic.put(ce.name(), ce.value());
                    }
                }
                allConfigs.computeIfAbsent(topicName, k -> new LinkedHashMap<>()).put(clusterName, dynamic);
            }
            LOG.info("[{}][{}] Collected {} user topics.", name, clusterName, userTopics.size());
        } catch (Exception e) {
            LOG.error("[{}][{}] Failed to collect cluster state", name, clusterName, e);
        }
    }

    private void applyDesiredState(String groupName, String clusterName, AdminClient client,
                                   Map<String, Integer> targetPartitionCount,
                                   Map<String, Map<String, String>> targetTopicConfigs,
                                   Map<String, TopicDescription> canonicalDescriptions, Properties clusterProps) {
        try {
            Set<String> existing =
                client.listTopics().names().get().stream().filter(t -> !isExcluded(t)).collect(Collectors.toSet());

            for (Map.Entry<String, Integer> entry : targetPartitionCount.entrySet()) {
                String topicName = entry.getKey();
                int targetPartitions = entry.getValue();
                Map<String, String> topicConfig = targetTopicConfigs.getOrDefault(topicName, Collections.emptyMap());

                if (!existing.contains(topicName)) {
                    createTopic(groupName, clusterName, client, topicName, targetPartitions,
                        canonicalDescriptions.get(topicName), topicConfig, clusterProps);
                } else {
                    syncPartitionCount(groupName, clusterName, client, topicName, targetPartitions);
                    syncTopicConfigs(groupName, clusterName, client, topicName, topicConfig);
                }
            }
        } catch (Exception e) {
            LOG.error("[{}][{}] Failed to apply desired state", name, clusterName, e);
        }
    }

    private void createTopic(String groupName, String clusterName, AdminClient client, String topicName,
                             int partitions, TopicDescription canonical, Map<String, String> configs,
                             Properties clusterProps) {
        short rf = deriveReplicationFactor(client, canonical);
        NewTopic newTopic = new NewTopic(topicName, partitions, rf);
        if (!configs.isEmpty()) {
            newTopic.configs(configs);
        }
        try {
            client.createTopics(Collections.singletonList(newTopic)).all().get();
            LOG.info("[{}][{}] Created topic '{}' partitions={} RF={} configs={}", groupName, clusterName, topicName,
                partitions, rf, configs.keySet());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOG.debug("[{}][{}] Topic '{}' already exists (race), will be handled next round.", groupName,
                    clusterName, topicName);
            } else {
                LOG.error("[{}][{}] Failed to create topic '{}'", groupName, clusterName, topicName, e);
            }
        } catch (Exception e) {
            LOG.error("[{}][{}] Failed to create topic '{}'", groupName, clusterName, topicName, e);
        }
    }

    // -------------------------------------------------------------------------
    // ACL sync
    // -------------------------------------------------------------------------

    private void syncPartitionCount(String groupName, String clusterName, AdminClient client, String topicName,
                                    int targetCount) {
        try {
            Map<String, TopicDescription> desc = client.describeTopics(Collections.singleton(topicName)).all().get();
            TopicDescription td = desc.get(topicName);
            if (td == null) {
                return;
            }
            int current = td.partitions().size();
            if (targetCount > current) {
                client.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(targetCount))).all().get();
                LOG.info("[{}][{}] Expanded partitions for '{}': {} → {}", groupName, clusterName, topicName, current
                    , targetCount);
            } else if (targetCount < current) {
                LOG.warn("[{}][{}] Topic '{}' has {} partitions but federation target is {}. " + "Kafka does not " +
                    "support partition reduction — skipping.", groupName, clusterName, topicName, current, targetCount);
            }
        } catch (Exception e) {
            LOG.error("[{}][{}] Failed to sync partition count for '{}'", groupName, clusterName, topicName, e);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void syncTopicConfigs(String groupName, String clusterName, AdminClient client, String topicName,
                                  Map<String, String> targetConfigs) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        try {
            Config currentConfig =
                client.describeConfigs(Collections.singletonList(resource)).all().get().get(resource);

            Map<String, String> currentDynamic = new HashMap<>();
            for (ConfigEntry ce : currentConfig.entries()) {
                if (ce.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                    currentDynamic.put(ce.name(), ce.value());
                }
            }

            List<AlterConfigOp> ops = new ArrayList<>();

            // SET / UPDATE entries present in target
            for (Map.Entry<String, String> kv : targetConfigs.entrySet()) {
                if (!kv.getValue().equals(currentDynamic.get(kv.getKey()))) {
                    ops.add(new AlterConfigOp(new ConfigEntry(kv.getKey(), kv.getValue()), AlterConfigOp.OpType.SET));
                }
            }

            // DELETE entries that exist locally but are absent from the target state
            for (String key : currentDynamic.keySet()) {
                if (!targetConfigs.containsKey(key)) {
                    ops.add(new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE));
                }
            }

            if (!ops.isEmpty()) {
                client.incrementalAlterConfigs(Collections.singletonMap(resource, ops)).all().get();
                LOG.info("[{}][{}] Synced configs for '{}': {}", groupName, clusterName, topicName,
                    ops.stream().map(op -> op.opType() + ":" + op.configEntry().name()).collect(Collectors.joining(","
                        + " ")));
            }
        } catch (Exception e) {
            LOG.error("[{}][{}] Failed to sync configs for '{}'", groupName, clusterName, topicName, e);
        }
    }

    /**
     * Merges all ACL bindings found across every cluster and applies the union to each cluster.
     * Only called when every cluster in the group has a non-PLAINTEXT security protocol.
     */
    private void syncAcls(String groupName, Map<String, AdminClient> adminClients) {
        LOG.info("[{}][{}] Syncing ACLs across {} clusters", name, groupName, adminClients.size());
        try {
            // Collect the union of all ACL bindings
            Set<AclBinding> unionAcls = new HashSet<>();
            for (Map.Entry<String, AdminClient> ce : adminClients.entrySet()) {
                try {
                    KafkaFuture<Collection<AclBinding>> future =
                        ce.getValue().describeAcls(AclBindingFilter.ANY).values();
                    Collection<AclBinding> bindings = future.get();
                    unionAcls.addAll(bindings);
                    LOG.info("[{}][{}][{}] Collected {} ACL bindings", name, groupName, ce.getKey(), bindings.size());
                } catch (Exception e) {
                    LOG.error("[{}][{}][{}] Failed to describe ACLs", name, groupName, ce.getKey(), e);
                }
            }

            if (unionAcls.isEmpty()) {
                LOG.info("[{}][{}] No ACL bindings found — nothing to sync.", name, groupName);
                return;
            }

            // Apply the union to each cluster
            for (Map.Entry<String, AdminClient> ce : adminClients.entrySet()) {
                String clusterName = ce.getKey();
                AdminClient client = ce.getValue();
                try {
                    Set<AclBinding> existing = new HashSet<>(client.describeAcls(AclBindingFilter.ANY).values().get());
                    List<AclBinding> toAdd =
                        unionAcls.stream().filter(acl -> !existing.contains(acl)).collect(Collectors.toList());
                    if (!toAdd.isEmpty()) {
                        client.createAcls(toAdd).all().get();
                        LOG.info("[{}][{}][{}] Added {} ACL binding(s)", name, groupName, clusterName, toAdd.size());
                    } else {
                        LOG.info("[{}][{}][{}] ACLs already up-to-date.", name, groupName, clusterName);
                    }
                } catch (Exception e) {
                    LOG.error("[{}][{}][{}] Failed to apply ACL bindings", name, groupName, clusterName, e);
                }
            }
        } catch (Exception e) {
            LOG.error("[{}][{}] Unexpected error during ACL sync", name, groupName, e);
        }
    }

    /**
     * Determine a safe replication factor for a new topic on the target cluster.
     */
    private short deriveReplicationFactor(AdminClient client, TopicDescription canonical) {
        short desired = 1;
        if (canonical != null && !canonical.partitions().isEmpty()) {
            desired = (short) canonical.partitions().get(0).replicas().size();
        }
        if (desired > maxReplicationFactor) {
            desired = maxReplicationFactor;
        }
        // Cap by actual broker count so creation never fails due to RF > brokers
        try {
            int brokerCount = client.describeCluster().nodes().get().size();
            if (desired > brokerCount) {
                desired = (short) brokerCount;
            }
        } catch (Exception e) {
            LOG.warn("Could not retrieve broker count for RF capping: {}", e.getMessage());
        }
        return desired < 1 ? 1 : desired;
    }

    /**
     * Build AdminClient properties from cluster-level config, ensuring the mandatory
     * {@code request.timeout.ms} is always present.
     */
    private Properties buildAdminProps(Properties clusterProps) {
        Properties p = new Properties();
        p.putAll(clusterProps);
        p.putIfAbsent("request.timeout.ms", String.valueOf(requestTimeoutMs));
        return p;
    }

    /**
     * A cluster is considered ACL-enabled when its security.protocol is not plain PLAINTEXT.
     */
    private boolean isAclEnabled(Properties props) {
        String protocol = props.getProperty("security.protocol", "PLAINTEXT").toUpperCase();
        return !protocol.equals("PLAINTEXT");
    }

    private boolean isExcluded(String topicName) {
        return topicName.startsWith("_") || excludedTopics.contains(topicName);
    }

    private void closeQuietly(AdminClient client) {
        try {
            client.close();
        } catch (Exception e) {
            LOG.warn("Error closing AdminClient: {}", e.getMessage());
        }
    }
}
