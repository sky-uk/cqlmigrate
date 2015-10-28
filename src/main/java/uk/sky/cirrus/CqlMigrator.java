package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.exception.ClusterUnhealthyException;
import uk.sky.cirrus.locking.Lock;
import uk.sky.cirrus.locking.LockConfig;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Utility class which helps to apply schema changes required by an application prior to application startup.
 * Makes use of a locking mechanism to ensure only one instance at a time can apply schema changes to the cassandra
 * database.
 *
 */

public final class CqlMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlMigrator.class);

    private final LockConfig lockConfig;

    public CqlMigrator(LockConfig lockConfig) {
        this.lockConfig = lockConfig;
    }

    /**
     * Allows cql migrate to be run on the command line.
     * Set hosts, keyspace and directories using system properties.
     *
     * @param args
     */
    public static void main(String[] args) {

        String hostsProperty = getProperty("hosts");
        String keyspaceProperty = getProperty("keyspace");
        String directoriesProperty = getProperty("directories");
        String port = getProperty("port");

        Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(java.nio.file.Paths::get)
                .collect(Collectors.toList());

        new CqlMigrator(LockConfig.builder().build())
                .migrate(hostsProperty.split(","), port == null ? 9042 : Integer.parseInt(port), keyspaceProperty, directories);
    }

    /**
     * If all nodes are up and a lock can be acquired this runs migration
     * starting with bootstrap.cql and then the rest in alphabetical order.
     *
     * @param hosts
     * @param port
     * @param keyspace
     * @param directories
     * @throws ClusterUnhealthyException                           if any nodes are down or the schema is not
     *                                                             in agreement before running migration
     * @throws CannotAcquireLockException                          if any of the queries to acquire lock fail or
     *                                                             {@link uk.sky.cirrus.locking.LockConfig.LockConfigBuilder#withTimeout(Duration)}
     *                                                             is reached before lock can be acquired.
     * @throws CannotReleaseLockException                          if any of the queries to release lock fail
     * @throws IllegalArgumentException                            if any file types other than .cql are found
     * @throws IllegalStateException                               if cql file has changed after migration has been run
     * @throws com.datastax.driver.core.exceptions.DriverException if any of the migration queries fails
     */
    public void migrate(String[] hosts, int port, String keyspace, Collection<Path> directories)
            throws ClusterUnhealthyException, CannotAcquireLockException, CannotReleaseLockException {

        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {

            ClusterHealth clusterHealth = new ClusterHealth(cluster);
            clusterHealth.check();

            Lock lock = Lock.acquire(lockConfig, keyspace, session);

            LOGGER.info("Loading cql files from {}", directories);
            Paths paths = Paths.create(directories);

            KeyspaceBootstrapper keyspaceBootstrapper = new KeyspaceBootstrapper(session, keyspace, paths);
            SchemaUpdates schemaUpdates = new SchemaUpdates(session, keyspace);
            SchemaLoader schemaLoader = new SchemaLoader(session, keyspace, schemaUpdates, paths);

            keyspaceBootstrapper.bootstrap();
            schemaUpdates.initialise();
            schemaLoader.load();

            lock.release();
        }
    }

    /**
     * Drops keyspace if it exists
     *
     * @param hosts
     * @param port
     * @param keyspace
     * @throws com.datastax.driver.core.exceptions.DriverException if query fails
     */
    public void clean(String[] hosts, int port, String keyspace) {
        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {

            session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
            LOGGER.info("Cleaned {}", keyspace);
        }
    }

    private Cluster createCluster(String[] hosts, int port) {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.ALL);

        return Cluster.builder()
                .addContactPoints(hosts)
                .withPort(port)
                .withQueryOptions(queryOptions)
                .build();
    }

    private static String getProperty(final String propertyName) {
        final String property = System.getProperty(propertyName);
        if (StringUtils.isEmpty(property)) {
            throw new IllegalArgumentException("Expected " + propertyName + " property to be set.");
        }
        return property;
    }
}