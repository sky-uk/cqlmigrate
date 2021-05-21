package uk.sky.cqlmigrate;


import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PreMigrationCheckerIntegrationTest {

    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    private static Cluster cluster;
    private static Session session;
    private static ClusterHealth clusterHealth;

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

        cluster = EmbeddedCassandraServerHelper.getCluster();
        clusterHealth = new ClusterHealth(cluster);
        session = EmbeddedCassandraServerHelper.getSession();
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void migrationNotNeededIfEverythingExists() throws Exception {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        cluster.connect(TEST_KEYSPACE).execute("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);");

        cluster.connect(TEST_KEYSPACE).execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('2015-04-01-13:56-create-status-table.cql', 'fa03a30eab18b64b74ee1ea7816e0513f03b4ac7', dateof(now()));");
        cluster.connect(TEST_KEYSPACE).execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('2015-04-01-13:57-add-column-to-status-table.cql', '93abe7ec3b14888b9a8bd8d680a30839a92a3248', dateof(now()));");
        cluster.connect(TEST_KEYSPACE).execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('2015-04-01-13:59-add-reference-data-to-status-table.cql', '75e456f43dd5e1b099091319bbf6bf047d8bc34b', dateof(now()));");
        Session session = cluster.connect(TEST_KEYSPACE);
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);

        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(Collections.singletonList(getResourcePath("cql_valid_one"))));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isFalse();
    }

    @Test
    public void migrationNeededIfKeyspaceDoesNotExist() {
        //given
        cluster.connect("system").execute("DROP KEYSPACE IF EXISTS " + TEST_KEYSPACE + ";");
        Session session = cluster.connect("system");
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);
        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(Collections.emptyList()));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }

    @Test
    public void migrationNeededIfSchemaUpdatesTableDoesNotExist() {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Session session = cluster.connect("system");
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);
        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(Collections.emptyList()));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }

    @Test
    public void shouldMigrateIfPreFlightChecksEnabledAndNoChangesApplied() throws Exception {
        //given
        Collection<Path> cqlPaths = new ArrayList<>();
        cqlPaths.add(getResourcePath("cql_valid_one"));

        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        cluster.connect(TEST_KEYSPACE).execute("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);");
        Session session = cluster.connect("system");
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);

        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(cqlPaths));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }

    @Test
    public void shouldMigrateIfPreFlightChecksEnabledAndSomeChangesNotApplied() throws Exception {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        cluster.connect(TEST_KEYSPACE).execute("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);");

        cluster.connect(TEST_KEYSPACE).execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('2015-04-01-13:56-create-status-table.cql', 'fa03a30eab18b64b74ee1ea7816e0513f03b4ac7', dateof(now()));");
        cluster.connect(TEST_KEYSPACE).execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('2015-04-01-13:57-add-column-to-status-table.cql', '93abe7ec3b14888b9a8bd8d680a30839a92a3248', dateof(now()));");
        Session session = cluster.connect("system");
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);

        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(Collections.singletonList(getResourcePath("cql_valid_one"))));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

}