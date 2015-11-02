package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SchemaUpdatesTest {

    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";
    private final Collection<String> CASSANDRA_HOSTS = Collections.singletonList("localhost");
    private final String TEST_KEYSPACE = "cqlmigrate_test";
    private final Cluster cluster = Cluster.builder().addContactPoints(CASSANDRA_HOSTS.toArray(new String[CASSANDRA_HOSTS.size()])).build();

    @Before
    public void setUp() throws Exception {
        cluster.connect().execute("drop keyspace if exists cqlmigrate_test");
    }

    @After
    public void tearDown() {
        cluster.closeAsync();
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @Test
    public void schemaUpdatesTableShouldBeCreatedIfNotExists() throws Exception {
        //given
        cluster.connect("system").execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Session session = cluster.connect(TEST_KEYSPACE);
        SchemaUpdates schemaUpdates = new SchemaUpdates(session, TEST_KEYSPACE);

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
        SchemaUpdates schemaUpdates = new SchemaUpdates(session, TEST_KEYSPACE);

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
