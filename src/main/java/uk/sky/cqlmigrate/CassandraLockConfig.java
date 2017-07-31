package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;

import java.time.Duration;

public class CassandraLockConfig extends LockConfig {

    private CassandraLockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure) {
        super(pollingInterval, timeout, clientId, unlockOnFailure);
    }

    @Override
    public LockingMechanism getLockingMechanism(Session session, String keySpace) {
        return new CassandraLockingMechanism(session, keySpace);
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
        public CassandraLockConfigBuilder unlockOnFailure() {
            super.unlockOnFailure();
            return this;
        }

        public CassandraLockConfig build() {
            return new CassandraLockConfig(pollingInterval, timeout, clientId, unlockOnFailure);
        }
    }
}
