package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import java.time.Duration;

public class CassandraLockConfig extends LockConfig {

    private final ConsistencyLevel consistencyLevel;
    private final String lockKeyspace;

    private CassandraLockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure, ConsistencyLevel consistencyLevel, String lockKeyspace) {
        super(pollingInterval, timeout, clientId, unlockOnFailure);
        this.consistencyLevel = consistencyLevel;
        this.lockKeyspace = lockKeyspace;
    }

    @Override
    public LockingMechanism getLockingMechanism(CqlSession session, String keySpace) {
        return new CassandraLockingMechanism(session, keySpace, consistencyLevel, lockKeyspace);
    }

    public static CassandraLockConfigBuilder builder() {
        return new CassandraLockConfigBuilder();
    }

    public ConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    public static class CassandraLockConfigBuilder extends LockConfig.LockConfigBuilder {
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_ONE;
        private String lockKeyspace = "cqlmigrate";

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

        public CassandraLockConfigBuilder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
            return this;
        }

        public CassandraLockConfigBuilder withLockKeyspace(String lockKeyspace) {
            this.lockKeyspace = lockKeyspace;
            return this;
        }

        public CassandraLockConfig build() {
            return new CassandraLockConfig(pollingInterval, timeout, clientId, unlockOnFailure, consistencyLevel, lockKeyspace);
        }
    }
}
