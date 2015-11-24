package uk.sky.cirrus.locking;

import com.google.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scassandra.cql.PrimitiveType;
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
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class CassandraLockingMechanismTest extends AbstractLockTest {
    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();

    private static final UUID CLIENT = UUID.randomUUID();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private CassandraLockingMechanism lockingMechanism;

    @Test
    public void shouldSetToConsistencyLevelAllWhenAcquiringLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                        .build()
        );
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG);

        //then
        Query expectedQuery = Query.builder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .build();

        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldSetToConsistencyLevelAllWhenReleasingLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                        .build()
        );
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("[applied]", true)))
                        .build()
        );

        //when
        Lock lock = Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG);
        lock.release();

        //then
        Query expectedQuery = Query.builder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .build();

        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldOnlyRetryAttemptToAcquireLockAfterConfiguredInterval() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", false)))
                        .build()
        );
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        final int pollingInterval = 50;
        final int timeout = 300;
        final LockConfig lockConfig = LockConfig.builder().withPollingInterval(ofMillis(pollingInterval)).withTimeout(ofMillis(timeout)).build();

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(lockingMechanism, lockConfig));

        //then
        assertThat(throwable).isNotNull();

        Query expectedQuery = Query.builder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .build();

        final Stream<Query> actualRetries = activityClient.retrieveQueries().stream().filter(query -> query.equals(expectedQuery));
        assertThat(actualRetries.count()).isEqualTo(1 + timeout / pollingInterval);

    }

    @Test
    public void shouldReturnLockToAClientIfItIsCurrentlyOwnedByItself() throws Throwable {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", CLIENT, "[applied]", false)))
                        .build()
        );
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        final LockConfig lockConfig = LockConfig.builder().withPollingInterval(ofMillis(50)).withTimeout(ofMillis(300)).build();

        //when
        Lock lock = Lock.acquire(lockingMechanism, lockConfig);

        //then
        assertThat(lock).isNotNull();

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
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG));

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query failed to execute");
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToExecuteWhenReleasingLock() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                        .build()
        );
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                        .withThen(then()
                                .withResult(PrimingRequest.Result.unavailable))
                        .build()
        );

        final Lock lock = Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG);

        //when
        Throwable throwable = catchThrowable(lock::release);

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotReleaseLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query failed to execute");
    }

    @Test
    public void shouldContinueWithoutFailingWhenStaleLockIsPassedToRelease() {

        final Query expectedDeleteQuery = Query.builder().withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?").build();

        // given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then().withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build());
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        final Lock activeLock = Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG);
        assertThat(activeLock).isNotNull();

        activeLock.release();

        final Lock staleLock = activeLock;
        staleLock.release();

        assertThat(activityClient.retrieveQueries().stream().filter(query -> query.equals(expectedDeleteQuery)).count()).isEqualTo(1);
    }

    @Test(timeout = 2000)
    public void shouldNotErrorWhenCurrentLockHolderIsRetryingAfterWriteTimeOutButDoesNotHoldLockNow() throws InterruptedException {

        final ExecutorService lockManager = Executors.newSingleThreadExecutor();

        // given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then().withResult(PrimingRequest.Result.write_request_timeout))
                .build());
        lockingMechanism = new CassandraLockingMechanism(session, LOCK_KEYSPACE, CLIENT);
        final Lock activeLock = Lock.acquire(lockingMechanism, DEFAULT_LOCK_CONFIG);

        // Attempt to release lock
        lockManager.submit(activeLock::release);

        primeDeleteForSuccess(UUID.randomUUID());

        lockManager.shutdown();
        lockManager.awaitTermination(3, TimeUnit.SECONDS);
    }

    private void primeDeleteForSuccess(final UUID newClientId) {
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", newClientId, "[applied]", false)))
                .build());
    }

    @Override
    public int getBinaryPort() {
        return BINARY_PORT;
    }

    @Override
    ScassandraServerRule getScassandra() {
        return SCASSANDRA;
    }
}
