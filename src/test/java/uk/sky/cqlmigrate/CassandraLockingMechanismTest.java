package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static java.lang.String.valueOf;
import static org.assertj.core.api.Assertions.*;

public class CassandraLockingMechanismTest {

    private static final String LOCK_KEYSPACE = "lock-keyspace";
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String PREPARE_INSERT_QUERY = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";
    private static final String PREPARE_DELETE_QUERY = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static final Server server = Server.builder().build();
    private static final ClusterSpec clusterSpec = ClusterSpec.builder().build();

    private static BoundCluster cluster;
    private static DataCenterSpec dc;

    private CqlSession session;
    private CassandraLockingMechanism lockingMechanism;

    Predicate<QueryLog> prepareQueryPredicate = i -> i.getFrame().message instanceof Prepare;

    @BeforeClass
    public static void classSetup() throws UnknownHostException {
        dc = clusterSpec.addDataCenter().withName("DC1").withCassandraVersion("3.11").build();
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
        cluster.acceptConnections();
        cluster.clearLogs();
        cluster.clearPrimes(true);

        session = newSession();
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, ConsistencyLevel.ALL, "cqlmigrate");
    }

    @After
    public void baseTearDown() {
        session.close();
    }

    @Test
    public void shouldPrepareInsertLocksQueryWhenInit() {
        //when
        lockingMechanism.init();
        //then
        List<QueryLog> queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(prepareQueryPredicate)
                .filter(queryLog -> ((Prepare) queryLog.getFrame().message).cqlQuery.equals(PREPARE_INSERT_QUERY))
                .collect(Collectors.toList());

        assertThat(queryLogs.size()).isEqualTo(1);
    }

    @Test
    public void shouldPrepareDeleteLocksQueryWhenInit() {
        //when
        lockingMechanism.init();

        //then
        List<QueryLog> queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
                .filter(prepareQueryPredicate).collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);
    }

    @Test
    public void shouldInsertLockWhenAcquiringLock() {

        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true));
        //when
        lockingMechanism.init();
        //then
        assertThat(lockingMechanism.acquire(CLIENT_ID)).isTrue();
    }

    @Test  // same as one above
    public void shouldSuccessfullyAcquireLockWhenInsertIsApplied() {
        // given
        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true));

        //when
        lockingMechanism.init();
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
                .describedAs("lock was acquired")
                .isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenInsertIsNotApplied() {
        //given
        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
                then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));

        //when
        lockingMechanism.init();
        //then
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
                .describedAs("lock was not acquired")
                .isFalse();
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyAcquired() {
        //given
        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, false));
        //when
        lockingMechanism.init();
        //then
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);
        assertThat(acquiredLock)
                .describedAs("lock was acquired")
                .isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenWriteTimeoutOccurs() {
        //given
        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
                then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));

        //when
        lockingMechanism.init();
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
                .describedAs("lock was not acquired")
                .isFalse();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenAcquiringLock() {
        cluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
                then(unavailable(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1)));
        lockingMechanism.init();
        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.acquire(CLIENT_ID));
        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage(String.format("Query to acquire lock %s.schema_migration for client %s failed to execute", LOCK_KEYSPACE, CLIENT_ID));
    }

    @Test
    public void shouldDeleteLockWhenReleasingLock() {
        //given
        cluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true));
        //when
        lockingMechanism.init();
        boolean isLockReleased = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(isLockReleased).isTrue();
        List<QueryLog> queryLogs = cluster.getLogs().getQueryLogs().stream()
                .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
                .collect(Collectors.toList());

        assertThat(queryLogs.size()).isEqualTo(1);

    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenNoLockFound() {
        cluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false));
        lockingMechanism.init();
        //when
        AbstractThrowableAssert<?, ? extends Throwable> execution = assertThatCode(
                () -> lockingMechanism.release(CLIENT_ID)
        );
        // then
        execution.doesNotThrowAnyException();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenReleasingLock() {
        //given
        cluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true).
                then(unavailable(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1)));
        lockingMechanism.init();

        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.release(CLIENT_ID));

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotReleaseLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query failed to execute");
    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenRetryingAfterWriteTimeOutButDoesNotHoldLockNow() {
        //given
        cluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false).
                then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));

        //when
        lockingMechanism.init();
        lockingMechanism.release(CLIENT_ID);

        //then prime a success
        cluster.clearPrimes(true);
        cluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true));

        //when
        boolean result = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(result)
                .describedAs("lock was released")
                .isTrue();
    }

    @Test
    public void shouldThrowCannotReleaseLockExceptionWhenLockNotHeldByUs() {
        //when
        String newLockHolder = "new lock holder";
        cluster.prime(primeDeleteQueryFailsWithLockForOtherClient(LOCK_KEYSPACE, CLIENT_ID, newLockHolder));
        //and
//        primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false);
//        cluster.prime(primeBuilder.then(noRows()));
        lockingMechanism.init();

        Throwable throwable = catchThrowable(() -> lockingMechanism.release(CLIENT_ID));

        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotReleaseLockException.class)
                .hasMessage(String.format("Lock %s.schema_migration attempted to be released by a non lock holder (%s). Current lock holder: %s", LOCK_KEYSPACE, CLIENT_ID, newLockHolder));
    }

    @Test // Not sure if this is a realistic possibility?
    public void shouldReturnFalseIfDeleteNotAppliedButClientIsUs() {
        //given
        cluster.prime(primeDeleteQueryFailsWithLockForOtherClient(LOCK_KEYSPACE, CLIENT_ID, CLIENT_ID));
        lockingMechanism.init();
        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released)
                .describedAs("lock was not released")
                .isFalse();
    }

    private CqlSession newSession() throws UnknownHostException {
        CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort)).withLocalDatacenter(dc.getName()).build();
        return session;
    }

    private static PrimeBuilder primeInsertQuery(String lockName, String clientId, Boolean lockApplied) {
        String prepareInsertQuery = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";

        PrimeBuilder primeBuilder = when(query(
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

    private static PrimeBuilder primeDeleteQuery(String lockName, String queriedClientId, boolean lockDeleted) {
        String deleteQuery = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

        return when(query(
                deleteQuery,
                Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
                new LinkedHashMap<>(ImmutableMap.of("name", lockName + ".schema_migration", "client", queriedClientId)),
                new LinkedHashMap<>(ImmutableMap.of("name", "varchar", "client", "varchar"))))
                .then(rows()
                        .row("[applied]", valueOf(lockDeleted)).columnTypes("[applied]", "boolean"));
    }

    private static PrimeBuilder primeDeleteQueryFailsWithLockForOtherClient(String lockName, String queriedClientId, String lockHeldClientId) {
        String deleteQuery = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

        return when(query(
                deleteQuery,
                Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
                new LinkedHashMap<>(ImmutableMap.of("name", lockName + ".schema_migration", "client", queriedClientId)),
                new LinkedHashMap<>(ImmutableMap.of("name", "varchar", "client", "varchar"))))
                .then(rows()
                        .row("[applied]", false, "client", lockHeldClientId).columnTypes("[applied]", "boolean", "clientid", "varchar"));
    }
}
