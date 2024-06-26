package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.google.common.hash.Hashing;
import com.google.common.io.Resources;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SchemaUpdatesTest {

    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";

    private static CqlSession session;
    private static ClusterHealth clusterHealth;
    private TableChecker tableChecker;

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

        session = EmbeddedCassandraServerHelper.getSession();
        clusterHealth = new ClusterHealth(session);
    }

    @Before
    public void setUp() {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        session.execute("CREATE KEYSPACE IF NOT EXISTS cqlmigrate_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        tableChecker = mock(TableChecker.class);
    }

    @After
    public void tearDown() {
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void schemaUpdatesTableShouldBeCreatedIfNotExists() {
        //given
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE, tableChecker);

        //when
        schemaUpdates.initialise();

        //then
        Optional<KeyspaceMetadata> keyspaceMetadata = session.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata).isNotEmpty();
        assertThat(keyspaceMetadata.get().getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
        verify(tableChecker).check(session, TEST_KEYSPACE);
    }

    @Test
    public void schemaUpdatesTableShouldNotBeCreatedIfExists() {
        //given
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE, tableChecker);

        //when
        schemaUpdates.initialise();

        try {
            schemaUpdates.initialise();
        } catch (AlreadyExistsException exception) {
            fail("Expected " + SCHEMA_UPDATES_TABLE + " table creation to be attempted only once.");
        }
        //then
        Optional<KeyspaceMetadata> keyspaceMetadata = session.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata).isNotEmpty();
        assertThat(keyspaceMetadata.get().getTable(SCHEMA_UPDATES_TABLE)).as("table should have been created").isNotNull();
    }

    /**
     * Make sure that the hashes that are calculated now using JDK builtins to what was previously calculated using
     * Guava's com.google.common.hash.Hashing library.
     *
     * @see Hashing
     */
    @Test
    public void rowInsertedWithMessageDigestHashingAlgorithmIsSameAsGuavaSha1HashingAlgorithm() throws Exception {
        //given
        SessionContext sessionContext = new SessionContext(session, ConsistencyLevel.ALL, ConsistencyLevel.ALL, clusterHealth);
        SchemaUpdates schemaUpdates = new SchemaUpdates(sessionContext, TEST_KEYSPACE, tableChecker);
        final String filename = "2018-03-26-18:11-create-some-tables.cql";
        final URL cqlResource = Resources.getResource("cql_schema_update_hashing/" + filename);
        schemaUpdates.initialise();

        //when
        schemaUpdates.add(
                filename,
                Paths.get(cqlResource.toURI())
        );

        //then
        final String guavaSha1Hash = Resources.asByteSource(cqlResource).hash(Hashing.sha1()).toString();
        final ResultSet resultSet = session.execute("SELECT * from " + SCHEMA_UPDATES_TABLE);
        Assertions.assertThat(resultSet.all())
                .hasOnlyOneElementSatisfying(row -> {
                    Assertions.assertThat(row.getString("filename"))
                            .isEqualTo(filename);
                    Assertions.assertThat(row.getString("checksum"))
                            .isEqualTo(guavaSha1Hash);
                });
    }
}
