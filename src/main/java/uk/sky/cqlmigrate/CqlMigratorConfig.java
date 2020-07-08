package uk.sky.cqlmigrate;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

public class CqlMigratorConfig {
    private final LockConfig cassandraLockConfig;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    private CqlMigratorConfig(LockConfig cassandraLockConfig, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        this.cassandraLockConfig = requireNonNull(cassandraLockConfig);
        this.readConsistencyLevel = requireNonNull(readConsistencyLevel);
        this.writeConsistencyLevel = requireNonNull(writeConsistencyLevel);
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

    public static class CassandraConfigBuilder {

        private LockConfig lockConfig;
        private ConsistencyLevel readConsistencyLevel;
        private ConsistencyLevel writeConsistencyLevel;

        private CassandraConfigBuilder() {}

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

        public CqlMigratorConfig build() {
            return new CqlMigratorConfig(lockConfig, readConsistencyLevel, writeConsistencyLevel);
        }
    }
}
