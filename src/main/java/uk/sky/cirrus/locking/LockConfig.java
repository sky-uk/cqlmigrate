package uk.sky.cirrus.locking;

import org.joda.time.Duration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LockConfig {

    private final Duration pollingInterval, timeout;
    private final ReplicationClass replicationClass;
    private final int replicationFactor;
    private final Map<String, Integer> dataCenters;

    private LockConfig(Duration pollingInterval, Duration timeout, ReplicationClass replicationClass, int replicationFactor, Map<String, Integer> dataCenters) {
        this.pollingInterval = pollingInterval;
        this.timeout = timeout;
        this.replicationClass = replicationClass;
        this.replicationFactor = replicationFactor;
        this.dataCenters = Collections.unmodifiableMap(dataCenters);
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public String getReplicationString() {
        switch (replicationClass) {
            case SimpleStrategy:
                return String.format("'class': '%s', 'replication_factor': %s", replicationClass, replicationFactor);

            case NetworkTopologyStrategy:
                StringBuilder replicationString = new StringBuilder("'class': 'NetworkTopologyStrategy', ");

                String delimiter = "";
                for (Map.Entry<String, Integer> dataCenter : dataCenters.entrySet()) {
                    replicationString
                            .append(delimiter)
                            .append("'")
                            .append(dataCenter.getKey())
                            .append("': ")
                            .append(dataCenter.getValue());

                    delimiter = ", ";
                }

                return replicationString.toString();

            default:
                throw new IllegalArgumentException("Unexpected replication class: " + replicationClass);
        }
    }

    public static LockConfigBuilder builder() {
        return new LockConfigBuilder();
    }

    public static class LockConfigBuilder {

        private Duration pollingInterval = Duration.millis(500);
        private Duration timeout = Duration.standardMinutes(1);
        private int replicationFactor = 1;
        private ReplicationClass replicationClass = ReplicationClass.SimpleStrategy;
        private Map<String, Integer> dataCenters = new HashMap<>();

        private LockConfigBuilder() {}

        public LockConfigBuilder withPollingInterval(Duration pollingInterval) {
            this.pollingInterval = pollingInterval;
            return this;
        }

        public LockConfigBuilder withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public LockConfigBuilder withSimpleStrategyReplication(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            this.replicationClass = ReplicationClass.SimpleStrategy;
            return this;
        }

        public LockConfigBuilder withNetworkTopologyReplication(String dataCenter, int replicationFactor) {
            dataCenters.put(dataCenter, replicationFactor);
            this.replicationClass = ReplicationClass.NetworkTopologyStrategy;
            return this;
        }

        public LockConfig build() {
            return new LockConfig(pollingInterval, timeout, replicationClass, replicationFactor, dataCenters);
        }
    }

    private enum ReplicationClass {
        SimpleStrategy, NetworkTopologyStrategy
    }
}
