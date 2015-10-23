package uk.sky.cirrus.locking;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;
import org.scassandra.http.client.types.ColumnMetadata;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cirrus.util.PortScavenger;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class LockTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final String LOCK_KEYSPACE = "lock-keyspace";

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
    }

    @After
    public void tearDown() throws Exception {
        cluster.closeAsync();
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
        Lock.acquire(LOCK_KEYSPACE, session);

        //then
        Query expectedQuery = Query.builder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withConsistency("ALL")
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
        Lock lock = Lock.acquire(LOCK_KEYSPACE, session);
        lock.release();

        //then
        Query expectedQuery = Query.builder()
                .withQuery("DELETE FROM locks.locks WHERE name = ?")
                .withConsistency("ALL")
                .build();

        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldOnlyRetryAttemptToAcquireLockEvery500Milliseconds() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withThen(then()
                        .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                        .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", false)))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(() -> Lock.acquire(LOCK_KEYSPACE, session));

        //then
        assertThat(throwable).isNotNull();

        Query expectedQuery = Query.builder()
                .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                .withConsistency("ALL")
                .build();

        assertThat(activityClient.retrieveQueries()).containsExactly(
                expectedQuery,
                expectedQuery,
                expectedQuery,
                expectedQuery,
                expectedQuery,
                expectedQuery
        );
    }

}