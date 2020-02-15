package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.server.AddressResolver;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Inet4Resolver;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.*;
import static java.lang.String.valueOf;
import static org.assertj.core.api.Assertions.*;

public class CassandraLockingMechanismSimulacron {

    private static final String LOCK_KEYSPACE = "lock-keyspace";
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String PREPARE_INSERT_QUERY = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";
    private static final String PREPARE_DELETE_QUERY = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

    private static final byte[] defaultStartingIp = new byte[] {127, 0, 0, 1};
    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static Server server;
    private static ClusterSpec clusterSpec;
    private static BoundCluster bCluster;
    private static CqlSession session;
    SocketAddress availableAddress;
    AddressResolver addressResolver;

    CassandraLockingMechanism lockingMechanism;

    Predicate<QueryLog> prepareQueryPredicate = i -> i.getFrame().message instanceof Prepare;

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
    }

    @After
    public void baseTearDown() throws Exception {
        bCluster.clearPrimes(true);
        addressResolver.release(availableAddress);
        server.close();
        session.close();
    }

    @Test
    public void shouldPrepareInsertLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();
        //then
        List<QueryLog> queryLogs = bCluster.getLogs().getQueryLogs().stream()
            .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_INSERT_QUERY))
            .filter(prepareQueryPredicate)
            .collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);
    }


    @Test
    public void shouldPrepareDeleteLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();

        //then
        List<QueryLog> queryLogs = bCluster.getLogs().getQueryLogs().stream()
            .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
            .filter(prepareQueryPredicate).collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);
    }

    @Test
    public void shouldInsertLockWhenAcquiringLock() throws Exception {

        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true));
        //when
        lockingMechanism.init();
        //then
        assertThat(lockingMechanism.acquire(CLIENT_ID)).isTrue();


    }

    //
//    @Test  // same as one above
//    public void shouldSuccessfullyAcquireLockWhenInsertIsApplied() throws Exception {
//        //when
//        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);
//
//        //then
//        assertThat(acquiredLock)
//            .describedAs("lock was acquired")
//            .isTrue();
//    }
//
    @Test
    public void shouldUnsuccessfullyAcquireLockWhenInsertIsNotApplied() throws Exception {
        //given
        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
            then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));
        //when
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, ConsistencyLevel.ALL);
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
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyAcquired() throws Exception {
        //given
        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, false));
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
        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
            then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)).ignoreOnPrepare());

        //when
        lockingMechanism.init();
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
            .describedAs("lock was not acquired")
            .isFalse();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenAcquiringLock() throws Exception {
        //given
        bCluster.prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
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
    public void shouldDeleteLockWhenReleasingLock() throws Exception {
        //given
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true));
        //when
        lockingMechanism.init();
        boolean isLockReleased = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(isLockReleased).isTrue();
        List<QueryLog> queryLogs = bCluster.getLogs().getQueryLogs().stream()
            .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
            .collect(Collectors.toList());

        assertThat(queryLogs.size()).isEqualTo(1);

    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenNoLockFound() throws Exception {
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false));
        lockingMechanism.init();
        //when
        AbstractThrowableAssert<?, ? extends Throwable> execution = assertThatCode(
            () -> lockingMechanism.release(CLIENT_ID)
        );
        // then
        execution.doesNotThrowAnyException();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenReleasingLock() throws Exception {
        //given
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true).
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

    @Ignore("needs review")
    @Test
    public void shouldSuccessfullyReleaseLockWhenRetryingAfterWriteTimeOutButDoesNotHoldLockNow() {
        //given
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true).
            then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));
        //when
        lockingMechanism.init();
        lockingMechanism.release(CLIENT_ID);
        //then prime a success
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false));
        //when
        boolean result = lockingMechanism.release(CLIENT_ID);
        //then
        assertThat(result)
            .describedAs("lock was released")
            .isTrue();
    }

    @Ignore("needs review")
    @Test
    public void shouldThrowCannotReleaseLockExceptionWhenLockNotHeldByUs() throws InterruptedException {
        //when
        String newLockHolder = "new lock holder";
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, newLockHolder, false).applyToPrepare());
        //and
        PrimeBuilder primeBuilder = primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false);
        bCluster.prime(primeBuilder.then(noRows()));
        lockingMechanism.init();

        Throwable throwable = catchThrowable(() -> lockingMechanism.release(CLIENT_ID));

        assertThat(throwable)
            .isNotNull()
            .isInstanceOf(CannotReleaseLockException.class)
            .hasMessage(String.format("Lock %s.schema_migration attempted to be released by a non lock holder (%s). Current lock holder: %s", LOCK_KEYSPACE, CLIENT_ID, newLockHolder));
    }

    @Test
    public void shouldReturnFalseIfDeleteNotAppliedButClientIsUs() {
        //given
        bCluster.prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false).applyToPrepare());
        lockingMechanism.init();
        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released)
            .describedAs("lock was not released")
            .isFalse();
    }

    private static PrimeBuilder primeInsertQuery(String lockName, String clientId, boolean lockApplied) {
        String prepareInsertQuery = "INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS";

        PrimeBuilder primeBuilder = when(query(
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

    private static PrimeBuilder primeDeleteQuery(String lockName, String clientId, boolean lockApplied) {
        String deleteQuery = "DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?";

        PrimeBuilder primeBuilder = when(query(
            deleteQuery,
            Lists.newArrayList(
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL),
            ImmutableMap.of("name", lockName + ".schema_migration", "client", clientId),
            ImmutableMap.of("name", "varchar", "client", "varchar")))
            .then(rows()
                .row("[applied]", valueOf(lockApplied), "client", CLIENT_ID).columnTypes("[applied]", "boolean", "clientid", "varchar"));
        return primeBuilder;
    }

}
