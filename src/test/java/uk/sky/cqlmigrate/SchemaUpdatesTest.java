package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SchemaUpdatesTest {

    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static int binaryPort;
    private static Cluster cluster;
    private static Session session;
    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        binaryPort = EmbeddedCassandraServerHelper.getNativeTransportPort();

        cluster = Cluster.builder().addContactPoints(CASSANDRA_HOSTS).withPort(binaryPort).build();
        session = cluster.connect();
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
    }

    @After
    public void tearDown() {
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        cluster.close();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void schemaUpdatesTableShouldBeCreatedIfNotExists() throws Exception {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Session session = cluster.connect(TEST_KEYSPACE);
        ExecutionInfo executionInfo = new ExecutionInfo(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL);
        SchemaUpdates schemaUpdates = new SchemaUpdates(executionInfo, TEST_KEYSPACE);

        //when
        schemaUpdates.initialise();

        //then
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata.getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
    }

    @Test
    public void schemaUpdatesTableShouldNotBeCreatedIfExists() throws Exception {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Session session = cluster.connect(TEST_KEYSPACE);
        ExecutionInfo executionInfo = new ExecutionInfo(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL);
        SchemaUpdates schemaUpdates = new SchemaUpdates(executionInfo, TEST_KEYSPACE);

        //when
        schemaUpdates.initialise();

        try {
            schemaUpdates.initialise();
        } catch (AlreadyExistsException exception) {
            fail("Expected " + SCHEMA_UPDATES_TABLE + " table creation to be attempted only once.");
        }
        //then
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata.getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
    }
}
