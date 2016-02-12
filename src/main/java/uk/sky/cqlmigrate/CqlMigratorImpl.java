package uk.sky.cqlmigrate;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Standard implementation for {@code CqlMigrator}
 * <p>
 * Implements Locking by providing mutex locking via the locks table.
 * Client tries to acquire the lock and would only succeed for one thread.
 * The inserted record will be deleted once the client releases the lock it holds, hence making way for a subsequent client thread to acquire the lock.
 * Locking is done per client per keyspace. A client can obtain a lock on a different keyspace if no other client is holding a lock for that keyspace.
 */
final class CqlMigratorImpl implements CqlMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlMigratorImpl.class);

    private final CqlMigratorConfig cqlMigratorConfig;

    CqlMigratorImpl(CqlMigratorConfig cqlMigratorConfig) {
        this.cqlMigratorConfig = cqlMigratorConfig;
    }


    /**
     * Allows cql migrate to be run on the command line.
     * Set hosts, keyspace and directories using system properties.
     */
    public static void main(String[] args) {
        String hosts = System.getProperty("hosts");
        String keyspace = System.getProperty("keyspace");
        String directoriesProperty = System.getProperty("directories");
        String port = System.getProperty("port");

        Preconditions.checkNotNull(hosts, "'hosts' property should be provided having value of a comma separated list of cassandra hosts");
        Preconditions.checkNotNull(keyspace, "'keyspace' property should be provided having value of the cassandra keyspace");
        Preconditions.checkNotNull(directoriesProperty, "'directories' property should be provided having value of the comma separated list of paths to cql files");

        Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(Paths::get)
                .collect(Collectors.toList());

        CqlMigratorFactory.create(CassandraLockConfig.builder().build())
                .migrate(hosts.split(","), port == null ? 9042 : Integer.parseInt(port), keyspace, directories);
    }


    /**
     * {@inheritDoc}
     */
    public void migrate(String[] hosts, int port, String keyspace, Collection<Path> directories) {
        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {
            this.migrate(session, keyspace, directories);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void migrate(Session session, String keyspace, Collection<Path> directories) {
        Cluster cluster = session.getCluster();
        ClusterHealth clusterHealth = new ClusterHealth(cluster);
        clusterHealth.check();

        CassandraLockingMechanism cassandraLockingMechanism = new CassandraLockingMechanism(session, keyspace);
        Lock lock = new Lock(cassandraLockingMechanism, cqlMigratorConfig.getCassandraLockConfig());
        lock.lock();

        LOGGER.info("Loading cql files from {}", directories);
        CqlPaths paths = CqlPaths.create(directories);

        ExecutionInfo executionInfo = new ExecutionInfo(session, cqlMigratorConfig.getReadConsistencyLevel(), cqlMigratorConfig.getWriteConsistencyLevel());

        KeyspaceBootstrapper keyspaceBootstrapper = new KeyspaceBootstrapper(executionInfo, keyspace, paths);
        SchemaUpdates schemaUpdates = new SchemaUpdates(executionInfo, keyspace);
        SchemaLoader schemaLoader = new SchemaLoader(executionInfo, keyspace, schemaUpdates, paths);

        keyspaceBootstrapper.bootstrap();
        schemaUpdates.initialise();
        schemaLoader.load();
        lock.unlock();
    }

    /**
     * {@inheritDoc}
     */
    public void clean(String[] hosts, int port, String keyspace) {
        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {
            this.clean(session, keyspace);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clean(Session session, String keyspace) {
        Statement clean = new SimpleStatement("DROP KEYSPACE IF EXISTS " + keyspace)
                .setConsistencyLevel(cqlMigratorConfig.getWriteConsistencyLevel());
        session.execute(clean);
        LOGGER.info("Cleaned {}", keyspace);
    }

    private Cluster createCluster(String[] hosts, int port) {
        return Cluster.builder()
                .addContactPoints(hosts)
                .withPort(port)
                .build();
    }
}
