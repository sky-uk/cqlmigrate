package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Time;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

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
    private final SessionContextFactory sessionContextFactory;

    CqlMigratorImpl(CqlMigratorConfig cqlMigratorConfig, SessionContextFactory sessionContextFactory) {
        this.cqlMigratorConfig = cqlMigratorConfig;
        this.sessionContextFactory = sessionContextFactory;
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
        String username = System.getProperty("username");
        String password = System.getProperty("password");

        requireNonNull(hosts, "'hosts' property should be provided having value of a comma separated list of cassandra hosts");
        requireNonNull(keyspace, "'keyspace' property should be provided having value of the cassandra keyspace");
        requireNonNull(directoriesProperty, "'directories' property should be provided having value of the comma separated list of paths to cql files");

        Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(Paths::get)
                .collect(Collectors.toList());
        CqlMigratorFactory.create(CassandraLockConfig.builder().build())
                .migrate(hosts.split(","), port == null ? 9042 : Integer.parseInt(port), username, password, keyspace, directories);

    }

    /**
     * {@inheritDoc}
     */
    public void migrate(String[] hosts, int port, String username, String password, String keyspace, Collection<Path> directories) {
        List<InetSocketAddress> cassandraHosts = Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());

        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoints(cassandraHosts)
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(username, password).build()) {
            this.migrate(cqlSession, keyspace, directories);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void migrate(CqlSession session, String keyspace, Collection<Path> directories) {
        LockingMechanism lockingMechanism = cqlMigratorConfig.getCassandraLockConfig().getLockingMechanism(session, keyspace);
        LockConfig lockConfig = cqlMigratorConfig.getCassandraLockConfig();

        boolean migrationFailed = false;
        Lock lock = new Lock(lockingMechanism, lockConfig);

        lock.lock();

        try {
            LOGGER.info("Loading cql files from {}", directories);
            CqlPaths paths = CqlPaths.create(directories);

            SessionContext sessionContext = sessionContextFactory.getInstance(session, cqlMigratorConfig);

            KeyspaceBootstrapper keyspaceBootstrapper = new KeyspaceBootstrapper(sessionContext, keyspace, paths);
            SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, keyspace);
            SchemaLoader schemaLoader = new SchemaLoader(sessionContext, keyspace, schemaUpdates, paths);

            keyspaceBootstrapper.bootstrap();
            schemaUpdates.initialise();
            schemaLoader.load();
        } catch (Exception e) {
            migrationFailed = true;
            throw e;
        } finally {
            lock.unlock(migrationFailed);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clean(String[] hosts, int port, String username, String password, String keyspace) {

        List<InetSocketAddress> cassandraHosts = Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());

        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoints(cassandraHosts)
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(username, password).build()) {
            this.clean(cqlSession, keyspace);
        }

    }

    /**
     * {@inheritDoc}
     */
    // TODO driver is taking longer than 2 secs to drop schema
    public void clean(Session session, String keyspace) {
        session.execute(SimpleStatement.newInstance("DROP KEYSPACE IF EXISTS " + keyspace)
                .setTimeout(Duration.ofSeconds(10))
                .setConsistencyLevel(cqlMigratorConfig.getWriteConsistencyLevel()), Statement.SYNC);

        LOGGER.info("Cleaned {}", keyspace);
    }

}
