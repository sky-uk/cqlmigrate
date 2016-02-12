package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;

public class CqlMigratorFactory {
    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     *
     * @param lockConfig with the desired properties for the lock handling
     * @return an instance of the CqlMigrator
     * @since 0.9.0
     * @deprecated since 0.9.1, replaced by {@link #create(CqlMigratorConfig)}
     */
    @Deprecated
    public static CqlMigrator create(CassandraLockConfig lockConfig) {
        return create(CqlMigratorConfig.builder()
                .cassandraLockConfig(lockConfig)
                .readConsistencyLevel(ConsistencyLevel.ALL)
                .writeConsistencyLevel(ConsistencyLevel.ALL)
                .build());
    }

    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     *
     * @param configuration with the desired properties for the lock handling and consistency levels
     * @return an instance of the CqlMigrator
     * @since 0.9.1
     */
    public static CqlMigrator create(CqlMigratorConfig configuration) {
        return new CqlMigratorImpl(configuration);
    }
}
