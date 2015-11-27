package uk.sky.cqlmigrate;

import java.time.Duration;

public class CassandraLockConfig extends LockConfig {

    private CassandraLockConfig(Duration pollingInterval, Duration timeout, String clientId) {
        super(pollingInterval, timeout, clientId);
    }

    public static CassandraLockConfigBuilder builder() {
        return new CassandraLockConfigBuilder();
    }

    public static class CassandraLockConfigBuilder extends LockConfig.LockConfigBuilder {

        private CassandraLockConfigBuilder() {}

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
            return new CassandraLockConfig(pollingInterval, timeout, clientId);
        }
    }
}
