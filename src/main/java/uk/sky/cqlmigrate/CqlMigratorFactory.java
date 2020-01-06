package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

public class CqlMigratorFactory {
    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     *
     * @param lockConfig with the desired properties for the lock handling
     * @return an instance of the CqlMigrator
     * @since 0.9.0
     */
    public static CqlMigrator create(LockConfig lockConfig) {
        return create(CqlMigratorConfig.builder()
            .withLockConfig(lockConfig)
            .withReadConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
            .withWriteConsistencyLevel(ConsistencyLevel.ALL)
            .build()
        );
    }

    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     *
     * @param cqlMigratorConfig with the desired properties for the lock handling and consistency levels for reasd/write
     * @return an instance of the CqlMigrator
     * @since 0.9.4
     */
    public static CqlMigrator create(CqlMigratorConfig cqlMigratorConfig) {
        return new CqlMigratorImpl(cqlMigratorConfig, new SessionContextFactory());
    }
}
