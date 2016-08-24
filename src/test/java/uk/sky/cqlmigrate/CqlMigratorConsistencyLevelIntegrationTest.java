package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;
import static org.scassandra.matchers.Matchers.containsQuery;

public class CqlMigratorConsistencyLevelIntegrationTest {

    @Rule
    public final ScassandraServerRule scassandra = new ScassandraServerRule(FREE_PORT, 1235);

    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static String username = "cassandra";
    private static String password = "cassandra";
    public static final int FREE_PORT = PortScavenger.getFreePort();
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    private final CassandraLockConfig lockConfig = CassandraLockConfig.builder()
            .withClientId(CLIENT_ID)
            .build();


    private PrimingClient primingClient = scassandra.primingClient();
    private ActivityClient activityClient = scassandra.activityClient();
    private static Session session;
    private Collection<Path> cqlPaths;

    @Before
    public void initialiseLockTable() throws ConfigurationException, IOException, TTransportException, InterruptedException, URISyntaxException {

        cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_consistency_level"));

        session = Cluster.builder()
                .addContactPoints(CASSANDRA_HOSTS)
                .withPort(FREE_PORT)
                .withCredentials(username, password)
                .build()
                .connect();

        primingClient.prime(queryBuilder()
            .withQuery("SELECT * FROM system.schema_keyspaces")
            .withThen(
                    then()
                    .withColumnTypes(column("keyspace_name", PrimitiveType.TEXT), column("durable_writes", PrimitiveType.BOOLEAN), column("strategy_class", PrimitiveType.VARCHAR), column("strategy_options", PrimitiveType.TEXT))
                    .withRows(ImmutableMap.of("keyspace_name", "cqlmigrate_test", "durable_writes", true, "strategy_class", "org.apache.cassandra.locator.SimpleStrategy", "strategy_options", "{\"replication_factor\":\"1\"}"))
            ));

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
    public void shouldApplyCorrectDefaultConsistencyLevelsConfiguredForUnderlyingQueries() throws Exception {

        //arrange
        ConsistencyLevel expectedDefaultReadConsistencyLevel = ConsistencyLevel.LOCAL_ONE;
        ConsistencyLevel expectedDefaultWriteConsistencyLevel = ConsistencyLevel.ALL;

        CqlMigrator migrator = CqlMigratorFactory.create(lockConfig);

        //act
        executeMigration(migrator, cqlPaths);

        //assert
        assertStatementsExecuteWithExpectedConsistencyLevels(expectedDefaultReadConsistencyLevel, expectedDefaultWriteConsistencyLevel);
    }

    private void executeMigration(CqlMigrator migrator, Collection<Path> cqlPaths) {
        migrator.migrate(CASSANDRA_HOSTS, FREE_PORT, username, password, TEST_KEYSPACE, cqlPaths);
    }

    @Test
    public void shouldApplyCorrectCustomisedConsistencyLevelsConfiguredForUnderlyingQueries() throws Exception {

        //arrange
        ConsistencyLevel expectedReadConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        ConsistencyLevel expectedWriteConsistencyLevel = ConsistencyLevel.EACH_QUORUM;

        CqlMigrator migrator = CqlMigratorFactory.create(CqlMigratorConfig.builder()
                .withCassandraLockConfig(lockConfig)
                .withReadConsistencyLevel(expectedReadConsistencyLevel)
                .withWriteConsistencyLevel(expectedWriteConsistencyLevel)
                .build()
        );

        //act
        executeMigration(migrator, cqlPaths);

        //assert
        assertStatementsExecuteWithExpectedConsistencyLevels(expectedReadConsistencyLevel, expectedWriteConsistencyLevel);


    }

    private void assertStatementsExecuteWithExpectedConsistencyLevels(ConsistencyLevel expectedReadConsistencyLevel, ConsistencyLevel expectedWriteConsistencyLevel) {
        //bootstrap.cql is already "applied" as we are priming cassandra to pretend it already has the keyspace

        //ensure that schema updates are applied at configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("CREATE TABLE consistency_test (column1 text primary key, column2 text)")
                .withConsistency(expectedWriteConsistencyLevel.toString())
                .build()));

        // ensure that any reads from schema updates are read at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("SELECT * FROM schema_updates where filename = ?")
                .withConsistency(expectedReadConsistencyLevel.toString())
                .build()));

        //ensure that any inserts into schema updates are done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES (?, ?, dateof(now()));")
                .withConsistency(expectedWriteConsistencyLevel.toString())
                .build()));

        //ensure that use keyspace is done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("USE cqlmigrate_test;")
                .withConsistency(expectedReadConsistencyLevel.toString())
                .build()));

        //ensure that create schema_updates table is done at the configured consistency level
        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
                .builder()
                .withQuery("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);")
                .withConsistency(expectedWriteConsistencyLevel.toString())
                .build()));
    }
}
