package uk.sky.cqlmigrate;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.assertj.core.groups.Tuple;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class PreMigrationCheckerIntegrationTest {

    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    private static CqlSession session;
    private static ClusterHealth clusterHealth;

    private static List<Tuple> allSchemaUpdates = new ArrayList<>();

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        session = EmbeddedCassandraServerHelper.getSession();
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        allSchemaUpdates.clear();
        allSchemaUpdates.add(new Tuple("2015-04-01-13:56-create-status-table.cql", "fa03a30eab18b64b74ee1ea7816e0513f03b4ac7"));
        allSchemaUpdates.add(new Tuple("2015-04-01-13:57-add-column-to-status-table.cql", "aae7184dd8c6814ee5071fbd95a31ef8409cfa9f"));
        allSchemaUpdates.add(new Tuple("2015-04-01-13:59-add-reference-data-to-status-table.cql", "75e456f43dd5e1b099091319bbf6bf047d8bc34b"));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }
//fail
    @Test
    public void migrationNotNeededIfEverythingExists() throws Exception {
        //given
        createKeyspace(TEST_KEYSPACE);
        createSchemaUpdatesTable(TEST_KEYSPACE);
        allSchemaUpdates.forEach(t -> insertSchemaUpdate(TEST_KEYSPACE, t));

        session.execute("USE " +TEST_KEYSPACE);
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
        session.execute("USE system");
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
        createKeyspace(TEST_KEYSPACE);
        session.execute("USE system");
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

        createKeyspace(TEST_KEYSPACE);
        createSchemaUpdatesTable(TEST_KEYSPACE);
        session.execute("USE system");
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaChecker schemaChecker = new SchemaChecker(sessionContext, TEST_KEYSPACE);

        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE, schemaChecker, CqlPaths.create(cqlPaths));

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }
//fail
    @Test
    public void shouldMigrateIfPreFlightChecksEnabledAndSomeChangesNotApplied() throws Exception {
        //given
        createKeyspace(TEST_KEYSPACE);
        createSchemaUpdatesTable(TEST_KEYSPACE);
        allSchemaUpdates.subList(0, 2).forEach(t -> insertSchemaUpdate(TEST_KEYSPACE, t));

        session.execute("USE system");
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

    private void createKeyspace(String keyspaceName) {
        session.execute("USE system");
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
    }

    private void createSchemaUpdatesTable(String keyspaceName) {
        session.execute("USE " +keyspaceName);
        session.execute("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);");
    }

    private void insertSchemaUpdate(String keyspaceName, Tuple filenameAndChecksum) {
        session.execute("USE " +keyspaceName);
        session.execute("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES ('" + filenameAndChecksum.toArray()[0] + "', '" + filenameAndChecksum.toArray()[1] + "', dateof(now()));");
    }

}