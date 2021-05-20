package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.junit.*;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class CqlMigratorConsistencyLevelIntegrationTest {

    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final int defaultStartingPort = PortScavenger.getFreePort();

    private static final Server server = Server.builder().build();
    private static final ClusterSpec clusterSpec = ClusterSpec.builder().build();
    private static final String username = "cassandra";
    private static final String password = "cassandra";
    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String LOCAL_DC = "DC1";

    private Collection<Path> cqlPaths;

    private static BoundCluster cluster;

    private final CassandraLockConfig lockConfig = CassandraLockConfig.builder().build();

    @BeforeClass
    public static void classSetup() throws UnknownHostException {
        DataCenterSpec dc = clusterSpec.addDataCenter().withName("DC1").withCassandraVersion("3.11").build();
        dc.addNode()
                .withAddress(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort))
                .withPeerInfo("host_id", UUID.randomUUID())
                .build();
        cluster = server.register(clusterSpec);

        cluster.prime(when("select cluster_name from system.local where key = 'local'")
                .then(rows().row("cluster_name", "0").build()));
    }

    @Before
    public void baseSetup() throws Exception {
        cluster.clearPrimes(true);
        cluster.clearLogs();

        setupKeyspace(TEST_KEYSPACE);
        initialiseLockTable();
    }

    @AfterClass
    public static void destroy() {
        cluster.close();
        server.close();
    }

    public void initialiseLockTable() throws ConfigurationException, URISyntaxException {
        cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_consistency_level"));

        cluster.prime(when("select cluster_name from system.local where key = 'local'")
                .then(rows().row("cluster_name", "0").build()));

        cluster.prime(primeInsertQuery(TEST_KEYSPACE, lockConfig.getClientId(), true));
        cluster.prime(primeDeleteQuery(TEST_KEYSPACE, lockConfig.getClientId(), true, UUID.randomUUID().toString()));
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
        String[] hosts = new String[]{"localhost"};
        migrator.migrate(hosts, LOCAL_DC, defaultStartingPort, username, password, TEST_KEYSPACE, cqlPaths);
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
        List<QueryLog> queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains("CREATE TABLE consistency_test (column1 text primary key, column2 text)"))
                .filter(queryLog -> queryLog.getConsistency().equals(toSimulacronConsistencyLevel(expectedWriteConsistencyLevel)))
                .collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);

        // ensure that any reads from schema updates are read at the configured consistency level
        queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains("SELECT * FROM schema_updates where filename = ?"))
                .filter(queryLog -> queryLog.getConsistency().equals(toSimulacronConsistencyLevel(expectedReadConsistencyLevel)))
                .collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);

        //ensure that any inserts into schema updates are done at the configured consistency level
        queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains("INSERT INTO schema_updates (filename, checksum, applied_on) VALUES (?, ?, dateof(now()));"))
                .filter(queryLog -> queryLog.getConsistency().equals(toSimulacronConsistencyLevel(expectedWriteConsistencyLevel)))
                .collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);

        //ensure that use keyspace is done at the configured consistency level
        queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains("USE cqlmigrate_test;"))
                .filter(queryLog -> queryLog.getConsistency().equals(toSimulacronConsistencyLevel(expectedReadConsistencyLevel)))
                .collect(Collectors.toList());
        assertThat(queryLogs.size()).isGreaterThanOrEqualTo(1);

        //ensure that create schema_updates table is done at the configured consistency level
        queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains("CREATE TABLE schema_updates (filename text primary key, checksum text, applied_on timestamp);"))
                .filter(queryLog -> queryLog.getConsistency().equals(toSimulacronConsistencyLevel(expectedWriteConsistencyLevel)))
                .collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);
    }

    private static PrimeDsl.PrimeBuilder primeInsertQuery(String lockName, String clientId, boolean lockApplied) {
        String prepareInsertQuery = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";

        PrimeDsl.PrimeBuilder primeBuilder = when(query(
                prepareInsertQuery,
                Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
                new LinkedHashMap<>(ImmutableMap.of("name", lockName + ".schema_migration", "client", clientId)),
                new LinkedHashMap<>(ImmutableMap.of("name", "varchar", "client", "varchar"))))
                .then(rows().row(
                        "[applied]", valueOf(lockApplied), "client", CLIENT_ID).columnTypes("[applied]", "boolean", "clientid", "varchar")
                );
        return primeBuilder;
    }

    private static PrimeDsl.PrimeBuilder primeDeleteQuery(String lockName, String clientId, boolean lockApplied, String lockHoldingClient) {
        String deleteQuery = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

        PrimeDsl.PrimeBuilder primeBuilder = when(query(
                deleteQuery,
                Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
                new LinkedHashMap<>(ImmutableMap.of("name", lockName + ".schema_migration", "client", clientId)),
                new LinkedHashMap<>(ImmutableMap.of("name", "varchar", "client", "varchar"))))
                .then(rows()
                        .row("[applied]", valueOf(lockApplied), "client", lockHoldingClient).columnTypes("[applied]", "boolean", "clientid", "varchar"));
        return primeBuilder;
    }

    private void setupKeyspace(String keyspaceName) {
        BoundCluster simulacron = cluster;
        Map<String, String> keyspaceColumns = ImmutableMap.of(
                "keyspace_name", "varchar",
                "durable_writes", "boolean",
                "replication", "map<varchar, varchar>");
        List<LinkedHashMap<String, Object>> allKeyspacesRows = new ArrayList<>();
        LinkedHashMap<String, Object> keyspaceRow = new LinkedHashMap<>();
        keyspaceRow.put("keyspace_name", keyspaceName);
        keyspaceRow.put("durable_writes", true);
        keyspaceRow.put(
                "replication",
                ImmutableMap.of(
                        "class", "org.apache.cassandra.locator.SimpleStrategy", "replication_factor", "1"));
        allKeyspacesRows.add(keyspaceRow);

        // prime the query the driver issues when fetching a single keyspace
        Query whenSelectKeyspace =
                new Query("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '" + keyspaceName + '\'');
        SuccessResult thenReturnKeyspace =
                new SuccessResult(Collections.singletonList(new LinkedHashMap<>(keyspaceRow)), new LinkedHashMap<>(keyspaceColumns));
        RequestPrime primeKeyspace = new RequestPrime(whenSelectKeyspace, thenReturnKeyspace);
        simulacron.prime(new Prime(primeKeyspace));

        // prime the query the driver issues when fetching all keyspaces
        Query whenSelectAllKeyspaces = new Query("SELECT * FROM system_schema.keyspaces");

        SuccessResult thenReturnAllKeyspaces = new SuccessResult(allKeyspacesRows, new LinkedHashMap<>(keyspaceColumns));
        RequestPrime primeAllKeyspaces =
                new RequestPrime(whenSelectAllKeyspaces, thenReturnAllKeyspaces);
        simulacron.prime(new Prime(primeAllKeyspaces));
    }
}
