package uk.sky.cirrus.locking;

import java.time.Duration;
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

    Duration getPollingInterval() {
        return pollingInterval;
    }

    Duration getTimeout() {
        return timeout;
    }

    String getReplicationString() {
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

        private Duration pollingInterval = Duration.ofMillis(500);
        private Duration timeout = Duration.ofMinutes(1);
        private int replicationFactor = 1;
        private ReplicationClass replicationClass = ReplicationClass.SimpleStrategy;
        private Map<String, Integer> dataCenters = new HashMap<>();

        private LockConfigBuilder() {}

        /**
         * Duration to wait after each attempt to acquire the lock.
         *
         * @param pollingInterval defaults to 500 milliseconds.
         * @return this
         * @throws IllegalArgumentException if value is less than 0
         */
        public LockConfigBuilder withPollingInterval(Duration pollingInterval) {
            if (pollingInterval.toMillis() < 0)
                throw new IllegalArgumentException("Polling interval must be positive: " + pollingInterval.toMillis());

            this.pollingInterval = pollingInterval;
            return this;
        }

        /**
         * Duration to attempt to acquire lock for.
         *
         * @param timeout defaults to 1 minute
         * @return this
         * @throws IllegalArgumentException if value is less than 0
         */
        public LockConfigBuilder withTimeout(Duration timeout) {
            if (timeout.toMillis() < 0)
                throw new IllegalArgumentException("Timeout must be positive: " + timeout.toMillis());

            this.timeout = timeout;
            return this;
        }

        /**
         * Sets the replication class to SimpleStrategy and replication factor to {@code replicationFactor}.
         *
         * @param replicationFactor defaults to 1
         * @return this
         * @throws IllegalArgumentException if data centers have been configured using {@link #withNetworkTopologyReplication(String, int)}
         */
        public LockConfigBuilder withSimpleStrategyReplication(int replicationFactor) {
            if (!dataCenters.isEmpty())
                throw new IllegalArgumentException("Replication class 'SimpleStrategy' cannot be used with data centers: " + dataCenters);

            this.replicationFactor = replicationFactor;
            this.replicationClass = ReplicationClass.SimpleStrategy;
            return this;
        }

        /**
         * Sets the replication class to NetworkTopologyStrategy and replication factor for each {@code dataCenter} to {@code replicationFactor}.
         * Can be called multiple times to add more data centers.
         * Default is SimpleStrategy with replication factor of 1.
         *
         * @param dataCenter
         * @param replicationFactor
         * @return this
         */
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
