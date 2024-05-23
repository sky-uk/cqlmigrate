package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        String localDC = System.getProperty("localDC");
        String keyspace = System.getProperty("keyspace");
        String directoriesProperty = System.getProperty("directories");
        int port = Integer.parseInt(System.getProperty("port", "9042"));
        String username = System.getProperty("username");
        String password = System.getProperty("password");
        String precheck = System.getProperty("precheck", "false");
        Duration tableCheckerInitDelay = Duration.parse(System.getProperty("tableCheckerInitDelay", "PT5S"));
        Duration tableCheckerTimeout = Duration.parse(System.getProperty("tableCheckerTimeout", "PT1M"));
        ConsistencyLevel readCL = DefaultConsistencyLevel.valueOf(System.getProperty("readCL", "LOCAL_ONE"));
        ConsistencyLevel writeCL = DefaultConsistencyLevel.valueOf(System.getProperty("writeCL", "ALL"));

        requireNonNull(hosts, "'hosts' property should be provided having value of a comma separated list of cassandra hosts");
        requireNonNull(localDC, "'localDC' property should be provided having value of local datacenter for the contact points mentioned in the hosts; " +
                "the local datacenter must be the same for all contact points");
        requireNonNull(keyspace, "'keyspace' property should be provided having value of the cassandra keyspace");
        requireNonNull(directoriesProperty, "'directories' property should be provided having value of the comma separated list of paths to cql files");

        Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(Paths::get)
                .collect(Collectors.toList());

        CqlMigratorConfig cqlMigratorConfig = CqlMigratorConfig.builder()
                .withLockConfig(CassandraLockConfig.builder().build())
                .withReadConsistencyLevel(readCL)
                .withWriteConsistencyLevel(writeCL)
                .withTableCheckerInitDelay(tableCheckerInitDelay)
                .withTableCheckerTimeout(tableCheckerTimeout)
                .build();

        CqlMigratorFactory.create(cqlMigratorConfig)
                .migrate(hosts.split(","), localDC, port, username, password, keyspace, directories, Boolean.parseBoolean(precheck));
    }

    /**
     * {@inheritDoc}
     */
    public void migrate(String[] hosts, String localDC, int port, String username, String password, String keyspace, Collection<Path> directories, boolean performPrechecks) {
        List<InetSocketAddress> cassandraHosts = Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());

        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoints(cassandraHosts)
                .withLocalDatacenter(localDC)
                .withAuthCredentials(username, password).build()) {
            this.migrate(cqlSession, keyspace, directories, performPrechecks);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void migrate(CqlSession session, String keyspace, Collection<Path> directories, boolean performPrechecks) {
        LockingMechanism lockingMechanism = cqlMigratorConfig.getCassandraLockConfig().getLockingMechanism(session, keyspace);
        LockConfig lockConfig = cqlMigratorConfig.getCassandraLockConfig();

        SessionContext sessionContext = sessionContextFactory.getInstance(session, cqlMigratorConfig);

        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, keyspace);
        TableChecker tableChecker = new TableCheckerFactory().getInstance(session, cqlMigratorConfig);

        LOGGER.info("Loading cql files from {}", directories);
        CqlPaths paths = CqlPaths.create(directories);

        if (performPrechecks) {
            PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, keyspace, schemaChecker, paths);
            if (!preMigrationChecker.migrationIsNeeded()) {
                LOGGER.info("Migration not needed as environment matches expected state");
                return;
            }
            LOGGER.info("Pre-migration checks completed, migration is needed. Continuing...");
        }

        boolean migrationFailed = false;
        Lock lock = new Lock(lockingMechanism, lockConfig);

        lock.lock();

        try {
            KeyspaceBootstrapper keyspaceBootstrapper = new KeyspaceBootstrapper(sessionContext, keyspace, paths);
            SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, keyspace, tableChecker);
            SchemaLoader schemaLoader = new SchemaLoader(sessionContext, keyspace, schemaUpdates, schemaChecker, tableChecker, paths);

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
    public void clean(String[] hosts, String localDC, int port, String username, String password, String keyspace) {

        List<InetSocketAddress> cassandraHosts = Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());

        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoints(cassandraHosts)
                .withLocalDatacenter(localDC)
                .withAuthCredentials(username, password).build()) {
            this.clean(cqlSession, keyspace);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clean(Session session, String keyspace) {
        session.execute(SimpleStatement.newInstance("DROP KEYSPACE IF EXISTS " + keyspace)
                .setConsistencyLevel(cqlMigratorConfig.getWriteConsistencyLevel()), Statement.SYNC);

        LOGGER.info("Cleaned {}", keyspace);
    }
}
