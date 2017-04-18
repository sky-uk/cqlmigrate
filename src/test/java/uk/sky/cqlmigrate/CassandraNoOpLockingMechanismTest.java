package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class CassandraNoOpLockingMechanismTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final String CLIENT_ID = UUID.randomUUID().toString();

    @ClassRule
    public static final ScassandraServerRule SCASSANDRA = new ScassandraServerRule(BINARY_PORT, ADMIN_PORT);

    @Rule
    public final ScassandraServerRule resetScassandra = SCASSANDRA;

    private final PrimingClient primingClient = SCASSANDRA.primingClient();
    private final ActivityClient activityClient = SCASSANDRA.activityClient();

    private Cluster cluster;
    private CassandraNoOpLockingMechanism lockingMechanism;

    @Before
    public void baseSetup() throws Exception {

        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(BINARY_PORT)
                .build();

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

        lockingMechanism = new CassandraNoOpLockingMechanism();
        lockingMechanism.init();

        activityClient.clearAllRecordedActivity();
    }

    @After
    public void baseTearDown() throws Exception {
        cluster.close();
    }

    @Test
    public void shouldPrepareNoInsertLocksQueryWhenInit() throws Throwable {
        //when
        lockingMechanism.init();

        //then
        assertThat(activityClient.retrievePreparedStatementPreparations()).isEmpty();
    }

    @Test
    public void nothingShouldInsertLockWhenAcquiringLock() throws Exception {
        //when
        lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(activityClient.retrievePreparedStatementExecutions()).isEmpty();
    }

    @Test
    public void shouldDeleteLockWhenReleasingLock() throws Exception {
        //when
        lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(activityClient.retrievePreparedStatementExecutions()).isEmpty();
    }

    @Test
    public void shouldAlwaysReleasingLock() throws Exception {
        //when
        boolean released = lockingMechanism.release(CLIENT_ID);

        //then
        assertThat(released).isTrue();
    }

    @Test
    public void shouldAlwaysAcquireLock() throws Exception {
        //when
        boolean acquired = lockingMechanism.acquire(CLIENT_ID);

        //then
        assertThat(acquired).isTrue();
    }
}
