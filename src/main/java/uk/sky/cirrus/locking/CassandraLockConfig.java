package uk.sky.cirrus.locking;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CassandraLockConfig extends LockConfig {

    private final ReplicationClass replicationClass;
    private final int replicationFactor;
    private final Map<String, Integer> dataCenters;

    private CassandraLockConfig(Duration pollingInterval, Duration timeout, String clientId, ReplicationClass replicationClass, int replicationFactor, Map<String, Integer> dataCenters) {
        super(pollingInterval, timeout, clientId);
        this.replicationClass = replicationClass;
        this.replicationFactor = replicationFactor;
        this.dataCenters = Collections.unmodifiableMap(dataCenters);
    }

    private enum ReplicationClass {
        SimpleStrategy, NetworkTopologyStrategy
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

    public static CassandraLockConfigBuilder builder() {
        return new CassandraLockConfigBuilder();
    }

    public static class CassandraLockConfigBuilder extends LockConfig.LockConfigBuilder {

        private int replicationFactor = 1;
        private ReplicationClass replicationClass = ReplicationClass.SimpleStrategy;
        private Map<String, Integer> dataCenters = new HashMap<>();

        private CassandraLockConfigBuilder() {}

        /**
         * Sets the replication class to SimpleStrategy and replication factor to {@code replicationFactor}.
         *
         * @param replicationFactor defaults to 1
         * @return this
         * @throws IllegalArgumentException if data centers have been configured using {@link #withNetworkTopologyReplication(String, int)}
         */
        public CassandraLockConfigBuilder withSimpleStrategyReplication(int replicationFactor) {
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
        public CassandraLockConfigBuilder withNetworkTopologyReplication(String dataCenter, int replicationFactor) {
            dataCenters.put(dataCenter, replicationFactor);
            this.replicationClass = ReplicationClass.NetworkTopologyStrategy;
            return this;
        }

        @Override
        public CassandraLockConfigBuilder withPollingInterval(Duration pollingInterval) {
            super.withPollingInterval(pollingInterval);
            return this;
        }

        @Override
        public CassandraLockConfigBuilder withTimeout(Duration timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public CassandraLockConfigBuilder withClientId(String clientId) {
            super.withClientId(clientId);
            return this;
        }

        public CassandraLockConfig build() {
            return new CassandraLockConfig(pollingInterval, timeout, clientId, replicationClass, replicationFactor, dataCenters);
        }
    }
}
