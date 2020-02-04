package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.*;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

import java.time.Duration;
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

    @ClassRule
    public static final SimulacronRule SIMULACRON_RULE = new SimulacronRule(ClusterSpec.builder().withNodes(1));
    private static CqlSession session;

    CassandraLockingMechanism lockingMechanism;

    Predicate<QueryLog> prepareQueryPredicate = i -> i.getFrame().message instanceof Prepare;

    @Before
    public void baseSetup() throws Exception {
        SIMULACRON_RULE.cluster().acceptConnections();
        SIMULACRON_RULE.cluster().clearLogs();
        SIMULACRON_RULE.cluster().clearPrimes(true);
        session = newSession(null);
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, ConsistencyLevel.ALL);
    }

    @After
    public void baseTearDown() throws Exception {
        session.close();
    }

    @Test
    public void shouldPrepareInsertLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();
        //then
        List<QueryLog> queryLogs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
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
        List<QueryLog> queryLogs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
            .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
            .filter(prepareQueryPredicate).collect(Collectors.toList());
        assertThat(queryLogs.size()).isEqualTo(1);
    }

    @Test
    public void shouldInsertLockWhenAcquiringLock() throws Exception {

        SIMULACRON_RULE.cluster().prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true));
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
        SIMULACRON_RULE.cluster().prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
            then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));

        try (CqlSession session = newSession(null)) {
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
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyAcquired() throws Exception {
        //given
        SIMULACRON_RULE.cluster().prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, false));
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
        SIMULACRON_RULE.cluster().prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
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
        SIMULACRON_RULE.cluster().prime(primeInsertQuery(LOCK_KEYSPACE, CLIENT_ID, true).
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
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true));
        //when
        lockingMechanism.init();
        boolean isLockReleased = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(isLockReleased).isTrue();
        List<QueryLog> queryLogs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
            .filter(queryLog -> queryLog.getFrame().message.toString().contains(PREPARE_DELETE_QUERY))
            .collect(Collectors.toList());

        assertThat(queryLogs.size()).isEqualTo(1);

    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenNoLockFound() throws Exception {
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false));
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
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true).
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
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, true).
            then(writeTimeout(com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ALL, 1, 1, WriteType.SIMPLE)));

        //when
        lockingMechanism.init();
        lockingMechanism.release(CLIENT_ID);

        //then prime a success
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false));

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
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, newLockHolder, false).applyToPrepare());
        //and
        PrimeBuilder primeBuilder = primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false);
        SIMULACRON_RULE.cluster().prime(primeBuilder.then(noRows()));
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
        SIMULACRON_RULE.cluster().prime(primeDeleteQuery(LOCK_KEYSPACE, CLIENT_ID, false).applyToPrepare());
        lockingMechanism.init();
        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released)
            .describedAs("lock was not released")
            .isFalse();
    }

    private CqlSession newSession(ProgrammaticDriverConfigLoaderBuilder loaderBuilder) {
        if (loaderBuilder == null) {
            loaderBuilder = SessionUtils.configLoaderBuilder();
            loaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");

        }
        DriverConfigLoader loader =
            loaderBuilder
                .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(1))
                .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMillis(500))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(2))
                .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(1))
                .build();
        return SessionUtils.newSession(SIMULACRON_RULE, loader);
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
