package uk.sky.cqlmigrate;

public class CqlMigratorFactory {
    /**
     * Creates an instance of CqlMigrator based on the provided configuration
     * @param configuration with the desired properties for the lock handling
     * @return an instance of the CqlMigrator working for Cassandra ?????
     */
    public static CqlMigrator create(CassandraLockConfig configuration) {
        return new CqlMigratorImpl(configuration);
    }
}
