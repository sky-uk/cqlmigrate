package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.AddressResolver;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Inet4Resolver;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.Query;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Predicate;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.query;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;
import static org.scassandra.matchers.Matchers.containsQuery;
import static uk.sky.cqlmigrate.CassandraNoOpLockingMechanismTestMock.CLIENT_ID;

public class CqlMigratorConsistencyLevelIntegrationTestSimulacron {

    private static final String LOCK_KEYSPACE = "lock-keyspace";
//    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final byte[] defaultStartingIp = new byte[] {127, 0, 0, 1};
    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static Server server;
    private static ClusterSpec clusterSpec;
    private static BoundCluster bCluster;
    private static CqlSession session;
    SocketAddress availableAddress;
    AddressResolver addressResolver;
    CassandraLockingMechanism lockingMechanism;

    private final CassandraLockConfig lockConfig = CassandraLockConfig.builder()
        .build();

    @Before
    public void baseSetup() throws Exception {
        server = Server.builder().build();
        clusterSpec = ClusterSpec.builder().build();
        DataCenterSpec dc = clusterSpec.addDataCenter().withCassandraVersion("3.8").build();
        addressResolver = new Inet4Resolver(defaultStartingIp, defaultStartingPort);
        availableAddress = addressResolver.get();

        dc.addNode().withAddress(availableAddress).build();
        bCluster = server.register(clusterSpec);
        session = CqlSession.builder().addContactPoint((InetSocketAddress) availableAddress).withLocalDatacenter(dc.getName()).build();
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, ConsistencyLevel.ALL);
        initialiseLockTable();
    }

    @After
    public void baseTearDown() throws Exception {
        bCluster.clearPrimes(true);
        addressResolver.release(availableAddress);
        server.close();
        session.close();
    }

//    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static String username = "cassandra";
    private static String password = "cassandra";
    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    private Collection<Path> cqlPaths;

    public void initialiseLockTable() throws ConfigurationException, URISyntaxException {

        cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_consistency_level"));

        bCluster.prime(primeSelectQuery());

        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, lockConfig.getClientId(), true).applyToPrepare());

        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, lockConfig.getClientId(), true).applyToPrepare());
    }

    @After
    public void tearDown() {
        session.close();
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

//        String[] hosts = {((InetSocketAddress) availableAddress).getHostName()};
        migrator.migrate(session, TEST_KEYSPACE, cqlPaths);
//        migrator.migrate(hosts, defaultStartingPort, username, password, TEST_KEYSPACE, cqlPaths);
    }

    @Test
    public void shouldApplyCorrectCustomisedConsistencyLevelsConfiguredForUnderlyingQueries() throws Exception {

        //arrange
        ConsistencyLevel expectedReadConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
        ConsistencyLevel expectedWriteConsistencyLevel = ConsistencyLevel.EACH_QUORUM;

        CqlMigrator migrator = CqlMigratorFactory.create(CqlMigratorConfig.builder()
            .withLockConfig(lockConfig)
            .withReadConsistencyLevel(expectedReadConsistencyLevel)
            .withWriteConsistencyLevel(expectedWriteConsistencyLevel)
            .build()
        );

        //act
        executeMigration(migrator, cqlPaths);

        //assert
        assertStatementsExecuteWithExpectedConsistencyLevels(expectedReadConsistencyLevel, expectedWriteConsistencyLevel);
    }

    private static com.datastax.oss.simulacron.common.codec.ConsistencyLevel toSimulacronConsistencyLevel(ConsistencyLevel driverConsistencyLevel) {
        return com.datastax.oss.simulacron.common.codec.ConsistencyLevel.fromString(driverConsistencyLevel.toString());
    }

    private void assertStatementsExecuteWithExpectedConsistencyLevels(ConsistencyLevel expectedReadConsistencyLevel, ConsistencyLevel expectedWriteConsistencyLevel) {
        //bootstrap.cql is already "applied" as we are priming cassandra to pretend it already has the keyspace

        //ensure that schema updates are applied at configured consistency level
        assertThat(bCluster.getLogs().getQueryLogs())
            .contains(
                new QueryLog(
                    "CREATE TABLE consistency_test (column1 text primary key, column2 text)",
                    toSimulacronConsistencyLevel(expectedWriteConsistencyLevel), null, null, 0L, 0L, false));

//        // ensure that any reads from schema updates are read at the configured consistency level
//        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
//            .builder()
//            .withQuery("SELECT * FROM schema_updates where filename = ?")
//            .withConsistency(expectedReadConsistencyLevel.toString())
//            .build()));
//
//        //ensure that any inserts into schema updates are done at the configured consistency level
//        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
//            .builder()
//            .withQuery("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES (?, ?, dateof(now()));")
//            .withConsistency(expectedWriteConsistencyLevel.toString())
//            .build()));
//
//        //ensure that use keyspace is done at the configured consistency level
//        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
//            .builder()
//            .withQuery("USE cqlmigrate_test;")
//            .withConsistency(expectedReadConsistencyLevel.toString())
//            .build()));
//
//        //ensure that create schema_updates table is done at the configured consistency level
//        Assert.assertThat(activityClient.retrieveQueries(), containsQuery(Query
//            .builder()
//            .withQuery("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);")
//            .withConsistency(expectedWriteConsistencyLevel.toString())
//            .build()));
    }

    private static PrimeDsl.PrimeBuilder primeSelectQuery() {
        String prepareSelectQuery = "SELECT * FROM system.schema_keyspaces";

        PrimeDsl.PrimeBuilder primeBuilder = when(query(
            prepareSelectQuery,
            Lists.newArrayList(
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL)))
            .then(rows()
                .columnTypes("keyspace_name", "text", "durable_writes", "boolean", "strategy_class", "varchar", "strategy_options", "text")
                .row("cqlmigrate_test", valueOf(true), "org.apache.cassandra.locator.SimpleStrategy", "{\"replication_factor\":\"1\"}")
            );
        return primeBuilder;
    }

    private static PrimeDsl.PrimeBuilder primeInsertQuery(String lockName, String clientId, boolean lockApplied) {
        String prepareInsertQuery = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";

        PrimeDsl.PrimeBuilder primeBuilder = when(query(
            prepareInsertQuery,
            Lists.newArrayList(
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
            ImmutableMap.of("name", lockName + ".schema_migration", "client", clientId),
            ImmutableMap.of("name", "varchar", "client", "varchar")))
            .then(rows().row(
                "[applied]", valueOf(lockApplied), "client", CLIENT_ID).columnTypes("[applied]", "boolean", "clientid", "varchar")
            );
        return primeBuilder;
    }

    private static PrimeDsl.PrimeBuilder primeDeleteQuery(String lockName, String clientId, boolean lockApplied) {
        return primeDeleteQuery(lockName, clientId, lockApplied, CLIENT_ID);
    }

    private static PrimeDsl.PrimeBuilder primeDeleteQuery(String lockName, String clientId, boolean lockApplied, String lockHoldingClient) {
        String deleteQuery = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

        PrimeDsl.PrimeBuilder primeBuilder = when(query(
            deleteQuery,
            Lists.newArrayList(
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
            ImmutableMap.of("name", lockName + ".schema_migration", "client", clientId),
            ImmutableMap.of("name", "varchar", "client", "varchar")))
            .then(rows()
                .row("[applied]", valueOf(lockApplied), "client", lockHoldingClient).columnTypes("[applied]", "boolean", "clientid", "varchar"));
        return primeBuilder;
    }
}
