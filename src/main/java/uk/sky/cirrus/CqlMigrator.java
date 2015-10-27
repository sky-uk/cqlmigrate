package uk.sky.cirrus;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.locking.Lock;
import uk.sky.cirrus.locking.LockConfig;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

public final class CqlMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlMigrator.class);

    private final LockConfig lockConfig;

    public CqlMigrator() {
        this.lockConfig = LockConfig.builder().build();
    }

    public CqlMigrator(LockConfig lockConfig) {
        this.lockConfig = lockConfig;
    }

    public static void main(String[] args) {

        String hostsProperty = getProperty("hosts");
        String keyspaceProperty = getProperty("keyspace");
        String directoriesProperty = getProperty("directories");

        Collection<String> hosts = Lists.newArrayList(hostsProperty.split(","));
        Collection<Path> directories = new ArrayList<>();
        for (String directoryString : directoriesProperty.split(",")) {
            directories.add(java.nio.file.Paths.get(directoryString));
        }

        new CqlMigrator().migrate(hosts, 9042, keyspaceProperty, directories);
    }

    public void migrate(Collection<String> hosts, int port, String keyspace, Collection<Path> directories) {

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

    public void clean(Collection<String> hosts, int port, String keyspace) {
        try (Cluster cluster = createCluster(hosts, port);
             Session session = cluster.connect()) {

            session.execute("DROP KEYSPACE IF EXISTS " + keyspace);
            LOGGER.info("Cleaned {}", keyspace);
        }
    }

    private Cluster createCluster(Collection<String> hosts, int port) {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.ALL);

        return Cluster.builder()
                .addContactPoints(hosts.toArray(new String[hosts.size()]))
                .withPort(port)
                .withQueryOptions(queryOptions)
                .build();
    }

    private static String getProperty(String propertyName) {
        String property = System.getProperty(propertyName);
        if (property == null || property.isEmpty())
            throw new IllegalArgumentException("Expected " + propertyName + " property to be set.");
        return property;
    }
}
