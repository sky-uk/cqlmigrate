package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import uk.sky.cirrus.exception.ClusterUnhealthyException;
import uk.sky.cirrus.util.PortScavenger;

import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.scassandra.http.client.PrimingRequest.then;

public class ClusterHealthTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final Collection<String> CASSANDRA_HOSTS = singletonList("localhost");

    private static final Scassandra scassandra = ScassandraFactory.createServer(BINARY_PORT, ADMIN_PORT);

    private static final PrimingClient primingClient = scassandra.primingClient();
    private static final ActivityClient activityClient = scassandra.activityClient();

    private Cluster cluster;
    private ClusterHealth clusterHealth;

    @Before
    public void setUp() throws Exception {
        scassandra.start();

        cluster = Cluster.builder()
                .addContactPoints(CASSANDRA_HOSTS.toArray(new String[CASSANDRA_HOSTS.size()]))
                .withPort(BINARY_PORT)
                .build();

        cluster.connect();

        clusterHealth = new ClusterHealth(cluster);

        primingClient.clearAllPrimes();

        activityClient.clearAllRecordedActivity();
    }

    @After
    public void tearDown() throws Exception {
        scassandra.stop();
        cluster.close();
    }

    @Test
    public void shouldThrowExceptionIfSchemaNotInAgreement() throws Exception {
        //given
        primingClient.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT peer, rpc_address, schema_version FROM system.peers")
                .withThen(then().withResult(PrimingRequest.Result.unavailable))
                .build()
        );

        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                clusterHealth.check();
            }
        });

        //then
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(ClusterUnhealthyException.class);
        assertThat(throwable).hasMessage("Cluster not healthy, schema not in agreement");
    }

    @Test
    public void shouldThrowExceptionIfHostIsDown() throws Exception {
        //given
        scassandra.stop();

        //when
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                clusterHealth.check();
            }
        });

        //then
        throwable.printStackTrace();
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(ClusterUnhealthyException.class);
        assertThat(throwable).hasMessage("Cluster not healthy, the following hosts are down: [localhost/127.0.0.1]");
    }
}
