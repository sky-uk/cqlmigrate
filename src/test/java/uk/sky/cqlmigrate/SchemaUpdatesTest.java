//package uk.sky.cqlmigrate;
//
//
//import com.datastax.oss.driver.api.core.CqlSession;
//import com.datastax.oss.driver.api.core.cql.ResultSet;
//import com.google.common.hash.Hashing;
//import com.google.common.io.Resources;
//import com.google.common.util.concurrent.Uninterruptibles;
//import org.apache.cassandra.exceptions.ConfigurationException;
//import org.apache.thrift.transport.TTransportException;
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
//import org.junit.*;
//
//import java.io.IOException;
//import java.net.URL;
//import java.nio.file.Paths;
//import java.util.concurrent.TimeUnit;
//
//import static org.assertj.core.api.Assertions.fail;
//
//public class SchemaUpdatesTest {
//
//    private static final String[] CASSANDRA_HOSTS = {"localhost"};
//    private static final String TEST_KEYSPACE = "cqlmigrate_test";
//    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";
//
//    private static int binaryPort;
//    private static String username = "cassandra";
//    private static String password = "cassandra";
//    private static CqlSession session;
//    private static ClusterHealth clusterHealth;
//
//
//    @BeforeClass
//    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
//        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
//
//        session = EmbeddedCassandraServerHelper.getSession();
//        clusterHealth = new ClusterHealth(session);
//        session = EmbeddedCassandraServerHelper.getSession();
//    }
//
//    @Before
//    public void setUp() throws Exception {
//        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
//    }
//
//    @After
//    public void tearDown() {
//        System.clearProperty("hosts");
//        System.clearProperty("keyspace");
//        System.clearProperty("directories");
//    }
//
//    @AfterClass
//    public static void tearDownClass() throws Exception {
//        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
//        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
//    }
//
//    @Test
//    public void schemaUpdatesTableShouldBeCreatedIfNotExists() throws Exception {
//        //given
//        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
//        CqlSession session = CqlSession.builder()n.connect(TEST_KEYSPACE);
//        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
//        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE);
//
//        //when
//        schemaUpdates.initialise();
//
//        //then
//        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
//        assertThat(keyspaceMetadata.getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
//    }
//
//    @Test
//    public void schemaUpdatesTableShouldNotBeCreatedIfExists() throws Exception {
//        //given
//        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
//        Session session = cluster.connect(TEST_KEYSPACE);
//        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
//        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE);
//
//        //when
//        schemaUpdates.initialise();
//
//        try {
//            schemaUpdates.initialise();
//        } catch (AlreadyExistsException exception) {
//            fail("Expected " + SCHEMA_UPDATES_TABLE + " table creation to be attempted only once.");
//        }
//        //then
//        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
//        assertThat(keyspaceMetadata.getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
//    }
//
//    /**
//     * Make sure that the hashes that are calculated now using JDK builtins to what was previously calculated using
//     * Guava's com.google.common.hash.Hashing library.
//     *
//     * @see Hashing
//     */
//    @Test
//    public void rowInsertedWithMessageDigestHashingAlgorithmIsSameAsGuavaSha1HashingAlgorithm() throws Exception {
//        //given
//        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
//        Session session = cluster.connect(TEST_KEYSPACE);
//        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
//        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE);
//        final String filename = "2018-03-26-18:11-create-some-tables.cql";
//        final URL cqlResource = Resources.getResource("cql_schema_update_hashing/" + filename);
//        schemaUpdates.initialise();
//
//        //when
//        schemaUpdates.add(
//            filename,
//            Paths.get(cqlResource.toURI())
//        );
//
//        //then
//        final String guavaSha1Hash = Resources.asByteSource(cqlResource).hash(Hashing.sha1()).toString();
//        final ResultSet resultSet = session.execute("SELECT * from " + SCHEMA_UPDATES_TABLE);
//        assertThat(resultSet.all())
//            .hasOnlyOneElementSatisfying(row -> {
//                assertThat(row.getString("filename"))
//                    .isEqualTo(filename);
//                assertThat(row.getString("checksum"))
//                    .isEqualTo(guavaSha1Hash);
//            });
//    }
//}
