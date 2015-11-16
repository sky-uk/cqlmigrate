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
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class LockTest extends AbstractLockTest {
    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();

    private static final UUID CLIENT = UUID.randomUUID();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

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

        //when
        Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);

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

        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build()
        );

        //when
        Lock lock = Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);
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

        final int pollingInterval = 50;
        final int timeout = 300;
        final LockConfig lockConfig = LockConfig.builder().withPollingInterval(ofMillis(pollingInterval)).withTimeout(ofMillis(timeout)).build();

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(lockConfig, LOCK_KEYSPACE, session, CLIENT));

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

        final LockConfig lockConfig = LockConfig.builder().withPollingInterval(ofMillis(50)).withTimeout(ofMillis(300)).build();

        //when
        Lock lock =  Lock.acquire(lockConfig, LOCK_KEYSPACE, session, CLIENT);

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

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT));

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

        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then()
                        .withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        final Lock lock = Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);

        //when
        Throwable throwable = catchThrowable(lock::release);

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotReleaseLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query failed to execute");
    }

    @Test
    public void shouldContinueWithoutFailingWhenStaleLockIsPassedToRelease(){

        final Query expectedDeleteQuery = Query.builder().withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?").build();

        // given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("DELETE FROM locks.locks WHERE name = ? IF client = ?")
                .withThen(then().withColumnTypes(ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("[applied]", true)))
                .build());

        final Lock activeLock = LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session);
        assertThat(activeLock).isNotNull();

        LockService.release(activeLock);

        final Lock staleLock = activeLock;
        LockService.release(staleLock);

        assertThat(activityClient.retrieveQueries().stream().filter(query -> query.equals(expectedDeleteQuery)).count()).isEqualTo(1);
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
