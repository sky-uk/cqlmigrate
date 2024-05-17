package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class CqlMigratorConfig {
    private final LockConfig cassandraLockConfig;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    private final Duration tableCheckerTimeout;

    private CqlMigratorConfig(LockConfig cassandraLockConfig, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel, Duration tableCheckerTimeout) {
        this.cassandraLockConfig = requireNonNull(cassandraLockConfig);
        this.readConsistencyLevel = requireNonNull(readConsistencyLevel);
        this.writeConsistencyLevel = requireNonNull(writeConsistencyLevel);
        this.tableCheckerTimeout = tableCheckerTimeout;
    }

    public static CassandraConfigBuilder builder() {
        return new CassandraConfigBuilder();
    }

    public LockConfig getCassandraLockConfig() {
        return cassandraLockConfig;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public Duration getTableCheckerTimeout() {
        return tableCheckerTimeout;
    }

    public static class CassandraConfigBuilder {

        private LockConfig lockConfig;
        private ConsistencyLevel readConsistencyLevel;
        private ConsistencyLevel writeConsistencyLevel;
        private Duration tableCheckerTimeout = Duration.ofMinutes(1);

        private CassandraConfigBuilder() {
        }

        public CassandraConfigBuilder withLockConfig(LockConfig cassandraLockConfig) {
            this.lockConfig = cassandraLockConfig;
            return this;
        }

        public CassandraConfigBuilder withReadConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
            this.readConsistencyLevel = readConsistencyLevel;
            return this;
        }

        public CassandraConfigBuilder withWriteConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
            this.writeConsistencyLevel = writeConsistencyLevel;
            return this;
        }

        public CassandraConfigBuilder withTableCheckerTimeout(Duration tableCheckerTimeout) {
            this.tableCheckerTimeout = tableCheckerTimeout;
            return this;
        }

        public CqlMigratorConfig build() {
            return new CqlMigratorConfig(lockConfig, readConsistencyLevel, writeConsistencyLevel, tableCheckerTimeout);
        }
    }
}
