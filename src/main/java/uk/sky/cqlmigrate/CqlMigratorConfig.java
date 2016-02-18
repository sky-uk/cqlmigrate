package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Preconditions;

class CqlMigratorConfig {
    private final CassandraLockConfig cassandraLockConfig;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    private CqlMigratorConfig(CassandraLockConfig cassandraLockConfig, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        Preconditions.checkNotNull(cassandraLockConfig);
        Preconditions.checkNotNull(readConsistencyLevel);
        Preconditions.checkNotNull(writeConsistencyLevel);

        this.cassandraLockConfig = cassandraLockConfig;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
    }

    static CassandraConfigBuilder builder() {
        return new CassandraConfigBuilder();
    }

    public CassandraLockConfig getCassandraLockConfig() {
        return cassandraLockConfig;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public static class CassandraConfigBuilder {

        private CassandraLockConfig cassandraLockConfig;
        private ConsistencyLevel readConsistencyLevel;
        private ConsistencyLevel writeConsistencyLevel;

        private CassandraConfigBuilder() {}

        public CassandraConfigBuilder withCassandraLockConfig(CassandraLockConfig cassandraLockConfig) {
            this.cassandraLockConfig = cassandraLockConfig;
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
            return new CqlMigratorConfig(cassandraLockConfig, readConsistencyLevel, writeConsistencyLevel);
        }
    }
}
