package uk.sky.cirrus.locking;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;
import org.scassandra.http.client.types.ColumnMetadata;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;
import uk.sky.cirrus.util.PortScavenger;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class CassandraLockingMechanismTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final String LOCK_KEYSPACE = "lock-keyspace";
    private static final LockConfig LOCK_CONFIG = LockConfig.builder().build();
    private static final UUID CLIENT = UUID.randomUUID();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private final PrimingClient primingClient = SCASSANDRA.primingClient();
    private final ActivityClient activityClient = SCASSANDRA.activityClient();

    private final Query deleteLockQuery = Query.builder()
            .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
            .build();
    private final Query insertLockQuery = Query.builder()
            .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
            .build();

    private Cluster cluster;
    private CassandraLockingMechanism lockingMechanism;

    @Before
    public void baseSetup() throws Exception {

        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(BINARY_PORT)
                .build();

        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", CLIENT, "[applied]", true)))
                .build()
        );

        lockingMechanism = new CassandraLockingMechanism(cluster.connect(), LOCK_KEYSPACE, CLIENT, LOCK_CONFIG);
    }

    @After
    public void baseTearDown() throws Exception {
        cluster.close();
    }

    @Test
    public void shouldThrowExceptionIfSelectKeyspaceQueryFailsWhenInit() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'locks'")
                .withThen(then().withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(lockingMechanism::init);

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldCreateLocksKeyspaceIfItDoesNotExistWhenInit() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'locks'")
                .withThen(then().withResult(PrimingRequest.Result.success))
                .build()
        );

        //when
        lockingMechanism.init();

        //then
        Query expectedQuery = Query.builder()
                .withQuery("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
                .build();

        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldThrowExceptionIfCreateLocksKeyspaceQueryFailsWhenInit() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
                .withThen(then().withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(lockingMechanism::init);

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldCreateLocksTableIfItDoesNotExistWhenInit() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'locks'")
                .withThen(then().withResult(PrimingRequest.Result.success))
                .build()
        );

        //when
        lockingMechanism.init();

        //then
        Query expectedQuery = Query.builder()
                .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                .build();

        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldThrowExceptionIfCreateLocksTableQueryFailsWhenInit() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                .withThen(then().withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(lockingMechanism::init);

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldInsertLockWhenAcquiringLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                        .build()
        );

        //when
        lockingMechanism.acquire();

        //then
        assertThat(activityClient.retrieveQueries()).contains(insertLockQuery);
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenInsertIsApplied() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire();

        //then
        assertThat(acquiredLock).isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenInsertIsNotApplied() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", false)))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire();

        //then
        assertThat(acquiredLock).isFalse();
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyAcquired() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", CLIENT, "[applied]", false)))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire();

        //then
        assertThat(acquiredLock).isTrue();
    }

    @Test
    public void shouldUnsuccessfullyAcquireLockWhenWriteTimeoutOccurs() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withResult(PrimingRequest.Result.write_request_timeout))
                .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire();

        //then
        assertThat(acquiredLock).isFalse();
    }

    @Test
    public void shouldSuccessfullyAcquireLockWhenLockIsAlreadyOwnedBySelf() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", CLIENT, "[applied]", false)))
                        .build()
        );

        //when
        boolean acquiredLock = lockingMechanism.acquire();

        //then
        assertThat(acquiredLock).isTrue();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenAcquiringLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withResult(PrimingRequest.Result.unavailable))
                        .build()
        );
        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.acquire());

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotAcquireLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query failed to execute");
    }

    @Test
    public void shouldDeleteLockWhenReleasingLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build()
        );

        //when
        lockingMechanism.release();

        //then
        assertThat(activityClient.retrieveQueries()).contains(deleteLockQuery);
    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenReleasingLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build()
        );

        //when
        boolean releasedLock = lockingMechanism.release();

        //then
        assertThat(releasedLock).isTrue();
    }

    @Test
    public void shouldSuccessfullyReleaseLockWhenNoLockFound() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", false)))
                .build()
        );

        //when
        boolean releasedLock = lockingMechanism.release();

        //then
        assertThat(releasedLock).isTrue();
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenReleasingLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                        .withThen(then()
                                .withResult(PrimingRequest.Result.unavailable))
                        .build()
        );

        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.release());

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotReleaseLockException.class)
                .hasCauseInstanceOf(DriverException.class)
                .hasMessage("Query failed to execute");
    }

    @Test(timeout = 2000)
    public void shouldSuccessfullyReleaseLockWhenRetryingAfterWriteTimeOutButDoesNotHoldLockNow() throws InterruptedException {
        //given
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then().withResult(PrimingRequest.Result.write_request_timeout))
                .build());

        //given
        executorService.submit(lockingMechanism::release);


        //then prime a success
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", false)))
                .build());

        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.SECONDS);

        assertThat(activityClient.retrieveQueries()).contains(deleteLockQuery, deleteLockQuery);
    }

    @Test
    public void shouldThrowCannotReleaseLockExceptionWhenLockNotHeldByUs() throws InterruptedException {
        //given
        UUID newLockHolder = UUID.randomUUID();
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", newLockHolder, "[applied]", false)))
                .build());

        //when
        Throwable throwable = catchThrowable(() -> lockingMechanism.release());

        //then
        assertThat(throwable)
                .isNotNull()
                .isInstanceOf(CannotReleaseLockException.class)
                .hasMessage(String.format("Lock attempted to be released by a non lock holder (%s). Current lock holder: %s", CLIENT, newLockHolder));
    }
}
