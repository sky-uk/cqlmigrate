package uk.sky.cqlmigrate;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.*;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

@Ignore("replaced with CassandraLockingMechanismSimulacron; can be removed/replaced after review")
public class CassandraLockingMechanismTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final String LOCK_KEYSPACE = "lock-keyspace";
    private static final String CLIENT_ID = UUID.randomUUID().toString();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private final PrimingClient primingClient = SCASSANDRA.primingClient();
    private final ActivityClient activityClient = SCASSANDRA.activityClient();

    private final PreparedStatementExecution deleteLockPreparedStatement = PreparedStatementExecution.builder()
            .withPreparedStatementText("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
            .withConsistency("ALL")
            .withVariables(LOCK_KEYSPACE + ".schema_migration", CLIENT_ID)
            .build();

    private final PreparedStatementExecution insertLockPreparedStatement = PreparedStatementExecution.builder()
            .withPreparedStatementText("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
            .withConsistency("ALL")
            .withVariables(LOCK_KEYSPACE + ".schema_migration", CLIENT_ID)
            .build();

        private CqlSession cluster;
    private CassandraLockingMechanism lockingMechanism;

    @Before
    public void baseSetup() throws Exception {

        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build()
        );

        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build()
        );

        lockingMechanism = new CassandraLockingMechanism(cluster, LOCK_KEYSPACE, ConsistencyLevel.ALL);
        lockingMechanism.init();

        activityClient.clearAllRecordedActivity();
    }

    @After
    public void baseTearDown() throws Exception {
        cluster.close();
    }

    @Test
    public void shouldPrepareInsertLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();

        //then
        PreparedStatementPreparation expectedPreparedStatement = PreparedStatementPreparation.builder()
                .withPreparedStatementText("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .build();

        assertThat(activityClient.retrievePreparedStatementPreparations()).contains(expectedPreparedStatement);
    }

    @Test
    public void shouldPrepareDeleteLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();

        //then
        PreparedStatementPreparation expectedPreparedStatement = PreparedStatementPreparation.builder()
                .withPreparedStatementText("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .build();

        assertThat(activityClient.retrievePreparedStatementPreparations()).contains(expectedPreparedStatement);
    }

    @Test
    public void shouldInsertLockWhenAcquiringLock() throws Exception {
        //when
        lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(activityClient.retrievePreparedStatementExecutions())
            .hasOnlyOneElementSatisfying(preparedStatementExecution -> {
                assertThat(preparedStatementExecution)
                    .usingRecursiveComparison()
                    .ignoringFields("variableTypes", "timestamp")
                    .isEqualTo(insertLockPreparedStatement);
            });
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenInsertIsApplied() throws Exception {
        //when
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
            .describedAs("lock was acquired")
            .isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenInsertIsNotApplied() throws Exception {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", "a different client", "[applied]", false)))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
            .describedAs("lock was not acquired")
            .isFalse();
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyAcquired() throws Exception {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", CLIENT_ID, "[applied]", false)))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
            .describedAs("lock was acquired")
            .isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenWriteTimeoutOccurs() throws Exception {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withResult(Result.write_request_timeout))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquiredLock)
            .describedAs("lock was not acquired")
            .isFalse();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenAcquiringLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                        .withQuery("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withResult(Result.unavailable))
                        .build()
        );
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
        //when
        lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(activityClient.retrievePreparedStatementExecutions())
            .hasOnlyOneElementSatisfying(preparedStatementExecution -> {
                assertThat(preparedStatementExecution)
                    .usingRecursiveComparison()
                    .ignoringFields("variableTypes", "timestamp")
                    .isEqualTo(deleteLockPreparedStatement);
            });
    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenNoLockFound() throws Exception {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", false)))
                .build()
        );

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
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                        .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                        .withThen(then()
                                .withResult(Result.unavailable))
                        .build()
        );

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
    public void shouldSuccessfullyReleaseLockWhenRetryingAfterWriteTimeOutButDoesNotHoldLockNow() throws InterruptedException {
        //given
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then().withResult(Result.write_request_timeout))
                .build());

        lockingMechanism.release(CLIENT_ID);


        //then prime a success
        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", "new lock holder", "[applied]", false)))
                .build()
        );

        //when
        boolean result = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(result)
            .describedAs("lock was released")
            .isTrue();
        assertThat(activityClient.retrievePreparedStatementExecutions())
            .hasSize(2)
            .allSatisfy(preparedStatementExecution -> assertThat(preparedStatementExecution)
                .usingRecursiveComparison()
                .ignoringFields("variableTypes", "timestamp")
                .isEqualTo(deleteLockPreparedStatement)
            );
    }

    @Test
    public void shouldThrowCannotReleaseLockExceptionWhenLockNotHeldByUs() throws InterruptedException {
        //given
        String newLockHolder = "new lock holder";

        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", newLockHolder, "[applied]", false)))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.release(CLIENT_ID));

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotReleaseLockException.class)
                .hasMessage(String.format("Lock %s.schema_migration attempted to be released by a non lock holder (%s). Current lock holder: %s", LOCK_KEYSPACE, CLIENT_ID, newLockHolder));
    }

    @Test
    public void shouldReturnFalseIfDeleteNotAppliedButClientIsUs() throws InterruptedException {
        //given

        primingClient.prime(PrimingRequest.preparedStatementBuilder()
                .withQuery("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withVariableTypes(PrimitiveType.TEXT, PrimitiveType.TEXT)
                        .withColumnTypes(column("client", PrimitiveType.TEXT), column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", CLIENT_ID, "[applied]", false)))
                .build()
        );

        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released)
            .describedAs("lock was not released")
            .isFalse();
    }
}
