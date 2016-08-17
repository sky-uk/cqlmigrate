package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import static com.google.common.base.Preconditions.*;

public class CqlMigratorConfig {
    private final CassandraLockConfig cassandraLockConfig;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    private CqlMigratorConfig(CassandraLockConfig cassandraLockConfig, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        this.cassandraLockConfig = checkNotNull(cassandraLockConfig);
        this.readConsistencyLevel = checkNotNull(readConsistencyLevel);
        this.writeConsistencyLevel = checkNotNull(writeConsistencyLevel);
    }

    public static CassandraConfigBuilder builder() {
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
