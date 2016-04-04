package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.Query;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;
import static org.scassandra.matchers.Matchers.containsQuery;

public class CqlMigratorConsistencyLevelIntegrationTest {

    @Rule
    public final ScassandraServerRule scassandra = new ScassandraServerRule(FREE_PORT, 1235);

    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    public static final int FREE_PORT = PortScavenger.getFreePort();
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    public static final ConsistencyLevel EXPECTED_READ_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_ONE;
    public static final ConsistencyLevel EXPECTED_WRITE_CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

    private final CassandraLockConfig lockConfig = CassandraLockConfig.builder()
            .withClientId(CLIENT_ID)
            .build();

    private final CqlMigrator migrator = CqlMigratorFactory.create(lockConfig);

    private PrimingClient primingClient = scassandra.primingClient();
    private ActivityClient activityClient = scassandra.activityClient();
    private static Session session;

    @Before
    public void initialiseLockTable() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        session = Cluster.builder()
                .addContactPoints(CASSANDRA_HOSTS)
                .withPort(FREE_PORT)
                .build()
                .connect();

        primingClient.prime(preparedStatementBuilder()
                .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(
                        then()
                                .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                                .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", CLIENT_ID, "[applied]", true)))
        );

        primingClient.prime(preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(
                        then()
                                .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                                .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", CLIENT_ID, "[applied]", true)))
        );
    }

    @After
    public void tearDown() {
        session.getCluster().close();
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void shouldApplyCorrectConsistencyLevelsConfiguredForUnderlyingQueries() throws Exception {

        //arrange
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_consistency_level"));

        //act
        migrator.migrate(CASSANDRA_HOSTS, FREE_PORT, TEST_KEYSPACE, cqlPaths);

        //assert

        //ensure bootstrap.cql is applied with configured write consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("CREATE KEYSPACE cqlmigrate_test  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
                .withConsistency(EXPECTED_WRITE_CONSISTENCY_LEVEL.toString())
                .build()));

        //ensure that schema updates are applied at configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("CREATE TABLE consistency_test (column1 text primary key, column2 text)")
                .withConsistency(EXPECTED_WRITE_CONSISTENCY_LEVEL.toString())
                .build()));

        // ensure that any reads from schema updates are read at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("SELECT * FROM schema_updates where filename = ?")
                .withConsistency(EXPECTED_READ_CONSISTENCY_LEVEL.toString())
                .build()));

        //ensure that any inserts into schema updates are done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES (?, ?, dateof(now()));")
                .withConsistency(EXPECTED_WRITE_CONSISTENCY_LEVEL.toString())
                .build()));

        //ensure that use keyspace is done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("USE cqlmigrate_test;")
                .withConsistency(EXPECTED_READ_CONSISTENCY_LEVEL.toString())
                .build()));

        //ensure that create schema_updates table is done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("CREATE TABLE IF NOT EXISTS schema_updates (filename text primary key, checksum text, applied_on timestamp);")
                .withConsistency(EXPECTED_WRITE_CONSISTENCY_LEVEL.toString())
                .build()));
    }
}
