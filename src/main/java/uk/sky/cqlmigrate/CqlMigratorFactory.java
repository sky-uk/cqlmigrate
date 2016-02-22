package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;

public class CqlMigratorFactory {
    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     *
     * @param lockConfig with the desired properties for the lock handling
     * @return an instance of the CqlMigrator
     * @since 0.9.0
     */
    public static CqlMigrator create(CassandraLockConfig lockConfig) {
        return new CqlMigratorImpl(CqlMigratorConfig.builder()
                .withCassandraLockConfig(lockConfig)
                .withReadConsistencyLevel(ConsistencyLevel.ALL)
                .withWriteConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
                .build());
    }
}
