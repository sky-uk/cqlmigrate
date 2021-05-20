package uk.sky.cqlmigrate;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
    public void migrationNotNeededIfEverythingExists() {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        cluster.connect(TEST_KEYSPACE).execute("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);");
        Session session = cluster.connect(TEST_KEYSPACE);
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE);

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
        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE);

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
        PreMigrationChecker preMigrationChecker = new PreMigrationChecker(sessionContext, TEST_KEYSPACE);

        //when
        boolean migrationIsNeeded = preMigrationChecker.migrationIsNeeded();

        //then
        assertThat(migrationIsNeeded).isTrue();
    }

}