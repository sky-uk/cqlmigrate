package uk.sky.cirrus.locking;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.ThrowableAssert;
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

import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.data.Index.atIndex;
import static org.scassandra.http.client.PrimingRequest.then;

public class LockTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final String LOCK_KEYSPACE = "lock-keyspace";
    private static final LockConfig DEFAULT_LOCK_CONFIG = LockConfig.builder().build();
    private static final String REPLICATION_CLASS = "SimpleStrategy";
    private static final int REPLICATION_FACTOR = 1;
    private static final UUID CLIENT = UUID.randomUUID();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private static final PrimingClient primingClient = SCASSANDRA.primingClient();
    private static final ActivityClient activityClient = SCASSANDRA.activityClient();

    private Cluster cluster;
    private Session session;

    @Before
    public void setUp() throws Exception {
        primingClient.clearAllPrimes();

        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(BINARY_PORT)
                .build();
        session = cluster.connect();

        activityClient.clearAllRecordedActivity();

        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                .build()
        );
    }

    @After
    public void tearDown() throws Exception {
        cluster.close();
    }

    @Test
    public void shouldTryToCreateLocksKeySpaceWithSimpleStrategy() throws Exception {
        //when
        Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);

        //then
        Query expectedQuery = Query.builder()
                .withQuery(String.format(
                        "CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': '%s', 'replication_factor': %s}",
                        REPLICATION_CLASS,
                        REPLICATION_FACTOR
                ))
                .build();
        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldTryToCreateLocksKeySpaceWithNetworkTopologyStrategy() throws Exception {
        //when
        LockConfig lockConfig = LockConfig.builder()
                .withNetworkTopologyReplication("DC1", 1)
                .withNetworkTopologyReplication("DC2", 2)
                .build();

        Lock.acquire(lockConfig, LOCK_KEYSPACE, session, CLIENT);

        //then
        Query expectedQuery = Query.builder()
                .withQuery("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': 'NetworkTopologyStrategy', 'DC2': 2, 'DC1': 1}")
                .build();
        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void ShouldThrowExceptionIfQueryFailsToCreateKeySpace() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery(String.format(
                        "CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': '%s', 'replication_factor': %s}",
                        REPLICATION_CLASS,
                        REPLICATION_FACTOR
                ))
                .withThen(then()
                        .withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);
            }
        });

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldTryToCreateLocksTable() throws Exception {
        //when
        Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);

        //then
        Query expectedQuery = Query.builder()
                .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                .build();
        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToCreateLocksTable() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                .withThen(then()
                        .withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);
            }
        });

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query to create locks schema failed to execute");
    }

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
                .withConsistency("QUORUM")
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
                .withQuery("DELETE FROM locks.locks WHERE name = ?")
                .build()
        );

        //when
        Lock lock = Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);
        lock.release();

        //then
        Query expectedQuery = Query.builder()
                .withQuery("DELETE FROM locks.locks WHERE name = ?")
                .withConsistency("QUORUM")
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

        final LockConfig lockConfig = LockConfig.builder().withPollingInterval(ofMillis(50)).withTimeout(ofMillis(300)).build();

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(lockConfig, LOCK_KEYSPACE, session, CLIENT));

        //then
        assertThat(throwable).isNotNull();

        Query expectedQuery = Query.builder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withConsistency("QUORUM")
                .build();

        assertThat(activityClient.retrieveQueries())
                .contains(expectedQuery, atIndex(2))
                .contains(expectedQuery, atIndex(3))
                .contains(expectedQuery, atIndex(4))
                .contains(expectedQuery, atIndex(5))
                .contains(expectedQuery, atIndex(6))
                .contains(expectedQuery, atIndex(7));
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
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);
            }
        });

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
                .withQuery("DELETE FROM locks.locks WHERE name = ?")
                .withThen(then()
                        .withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        final Lock lock = Lock.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session, CLIENT);

        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {            @Override
            public void call() throws Throwable {
                lock.release();
            }
        });

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotReleaseLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query failed to execute");
    }
}
