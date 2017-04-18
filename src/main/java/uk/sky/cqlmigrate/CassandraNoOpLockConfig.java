package uk.sky.cqlmigrate;

import java.time.Duration;

public class CassandraNoOpLockConfig extends LockConfig {

    private CassandraNoOpLockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure) {
        super(pollingInterval, timeout, clientId, unlockOnFailure);
    }

    public static CassandraNoOpLockConfigBuilder builder() {
        return new CassandraNoOpLockConfigBuilder();
    }

    public static class CassandraNoOpLockConfigBuilder extends LockConfigBuilder {

        private CassandraNoOpLockConfigBuilder() {}

        @Override
        public CassandraNoOpLockConfigBuilder withPollingInterval(Duration pollingInterval) {
            super.withPollingInterval(pollingInterval);
            return this;
        }

        @Override
        public CassandraNoOpLockConfigBuilder withTimeout(Duration timeout) {
            super.withTimeout(timeout);
            return this;
        }

        @Override
        public CassandraNoOpLockConfigBuilder unlockOnFailure() {
            super.unlockOnFailure();
            return this;
        }

        public CassandraNoOpLockConfig build() {
            return new CassandraNoOpLockConfig(pollingInterval, timeout, clientId, unlockOnFailure);
        }
    }
}
