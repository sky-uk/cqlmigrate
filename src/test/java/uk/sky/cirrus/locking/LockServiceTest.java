package uk.sky.cirrus.locking;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Query;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.util.PortScavenger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class LockServiceTest extends AbstractLockTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    @After
    public void tearDown(){
        LockService.SCHEMA_CREATED.set(false);
    }

    @Test
    public void shouldTryToCreateLocksKeySpaceWithSimpleStrategy() throws Exception {
        //when
        LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session);

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

        LockService.acquire(lockConfig, LOCK_KEYSPACE, session);

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
        Throwable throwable = catchThrowable(() -> LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session));

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldTryToCreateLocksTable() throws Exception {
        //when
        LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session);

        //then
        Query expectedQuery = Query.builder()
                .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                .build();
        assertThat(activityClient.retrieveQueries()).contains(expectedQuery);
    }

    @Test
    public void shouldThrowExceptionIfQueryFailsToCreateLocksTable() throws Exception {
        //given
        LockService.SCHEMA_CREATED.set(false);
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)")
                        .withThen(then()
                                .withResult(PrimingRequest.Result.unavailable))
                        .build()
        );

        //when
        Throwable throwable = catchThrowable(() -> LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session));

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause()).isNotNull();
        assertThat(throwable).hasMessage("Query to create locks schema failed to execute");
    }

    @Test
    public void shouldSkipSchemaCreationIfLockSchemaAlreadyExists() throws Throwable {
        Query[] notExpectedQueries = new Query[]{
                Query.builder().withQuery("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)").build(),
                Query.builder().withQuery("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };").build()
        };

        //given
        LockService.SCHEMA_CREATED.set(false);
        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'locks'")
                        .withThen(then()
                                .withRows(ImmutableMap.of()))
                        .build()
        );

        //when
        LockService.acquire(DEFAULT_LOCK_CONFIG, LOCK_KEYSPACE, session);

        //then
        assertThat(LockService.SCHEMA_CREATED.get()).isEqualTo(true);
        assertThat(activityClient.retrieveQueries()).doesNotContain(notExpectedQueries);


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