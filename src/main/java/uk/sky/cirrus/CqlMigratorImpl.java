package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.exception.ClusterUnhealthyException;
import uk.sky.cirrus.locking.Lock;
import uk.sky.cirrus.locking.LockConfig;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.nio.file.Path;
import java.nio.file.Paths;
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

public final class CqlMigratorImpl implements CqlMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlMigratorImpl.class);

    private final LockConfig lockConfig;

    public CqlMigratorImpl(LockConfig lockConfig) {
        this.lockConfig = lockConfig;
    }

    /**
     * Allows cql migrate to be run on the command line.
     * Set hosts, keyspace and directories using system properties.
     *
     * @param args
     */
    public static void main(String[] args) {

        final String hosts = System.getProperty("hosts");
        final String keyspace = System.getProperty("keyspace");
        final String directoriesProperty = System.getProperty("directories");
        final String port = System.getProperty("port");

        Preconditions.checkNotNull(hosts, "'hosts' property should be provided having value of a comma separated list of cassandra hosts");
        Preconditions.checkNotNull(keyspace, "'keyspace' property should be provided having value of the cassandra keyspace");
        Preconditions.checkNotNull(directoriesProperty, "'directories' property should be provided having value of the comma separated list of paths to cql files");

        final Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(Paths::get)
                .collect(Collectors.toList());

        new CqlMigratorImpl(LockConfig.builder().build())
                .migrate(hosts.split(","), port == null ? 9042 : Integer.parseInt(port), keyspace, directories);
    }

    /**
     * If all nodes are up and a lock can be acquired this runs migration
     * starting with bootstrap.cql and then the rest in alphabetical order.
     *
     * @param hosts  Comma separated list of cassandra hosts
     * @param port   Native transport port for the above cassandra nodes
     * @param keyspace  Keyspace name for which the schema migration needs to be applied
     * @param directories  Comma separated list of directory paths containing the cql statements for the schema change
     *
     * @throws ClusterUnhealthyException  if any nodes are down or the schema is not in agreement before running migration
     * @throws CannotAcquireLockException  if any of the queries to acquire lock fail or
     *                                     {@link uk.sky.cirrus.locking.LockConfig.LockConfigBuilder#withTimeout(Duration)}
     *                                     is reached before lock can be acquired.
     * @throws CannotReleaseLockException  if any of the queries to release lock fail
     * @throws IllegalArgumentException  if any file types other than .cql are found
     * @throws IllegalStateException  if cql file has changed after migration has been run
     * @throws com.datastax.driver.core.exceptions.DriverException  if any of the migration queries fails
     */
    public void migrate(String[] hosts, int port, String keyspace, Collection<Path> directories) {

        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {
            this.migrate(session, keyspace, directories);
        }
    }

    /**
     * @param session  Session to a cassandra cluster
     * @param keyspace  Keyspace name for which the schema migration needs to be applied
     * @param directories  Comma separated list of directory paths containing the cql statements for the schema change
     *
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
    public void migrate(Session session, String keyspace, Collection<Path> directories) {
        final Cluster cluster = session.getCluster();
        ClusterHealth clusterHealth = new ClusterHealth(cluster);
        clusterHealth.check();

        Lock lock = Lock.acquire(lockConfig, keyspace, session);

        LOGGER.info("Loading cql files from {}", directories);
        CqlPaths paths = CqlPaths.create(directories);

        final KeyspaceBootstrapper keyspaceBootstrapper = new KeyspaceBootstrapper(session, keyspace, paths);
        final SchemaUpdates schemaUpdates = new SchemaUpdates(session, keyspace);
        final SchemaLoader schemaLoader = new SchemaLoader(session, keyspace, schemaUpdates, paths);

        keyspaceBootstrapper.bootstrap();
        schemaUpdates.initialise();
        schemaLoader.load();
        lock.release();
    }

    /**
     * Drops keyspace if it exists
     *
     * @param hosts  Comma separated list of cassandra hosts
     * @param port   Native transport port for the above cassandra nodes
     * @param keyspace  Keyspace name for which the schema migration needs to be applied
     *
     * @throws com.datastax.driver.core.exceptions.DriverException if query fails
     */
    public void clean(String[] hosts, int port, String keyspace) {
        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {
            this.clean(session, keyspace);
        }
    }

    public void clean(Session session, String keyspace) {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
        LOGGER.info("Cleaned {}", keyspace);
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
}