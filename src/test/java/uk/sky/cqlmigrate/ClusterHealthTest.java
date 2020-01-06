package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.InetSocketAddress;
import java.util.Collection;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

public class ClusterHealthTest {

    private static final int BINARY_PORT = 9042;
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static final Collection<String> CASSANDRA_HOSTS = singletonList("localhost");
    private static final Collection<InetSocketAddress> CASSANDRA_NODES = singletonList(new InetSocketAddress("localhost", BINARY_PORT));

    private static final Scassandra scassandra = ScassandraFactory.createServer("0.0.0.0",BINARY_PORT, "0.0.0.0",ADMIN_PORT);

    private static final PrimingClient primingClient = scassandra.primingClient();
    private static final ActivityClient activityClient = scassandra.activityClient();

    private CqlSession cluster;
    private ClusterHealth clusterHealth;

    @Before
    public void setUp() {
        scassandra.start();

        String username = "cassandra";
        String password = "cassandra";
        cluster = CqlSession.builder()
            .addContactPoints(CASSANDRA_NODES)
            .withAuthCredentials(username, password)
            .build();

        clusterHealth = new ClusterHealth(cluster);

        primingClient.clearAllPrimes();

        activityClient.clearAllRecordedActivity();
    }

    @After
    public void tearDown() {
        scassandra.stop();
        cluster.close();
    }

    @Test
    public void shouldThrowExceptionIfHostIsDown() {
        //given
        scassandra.stop();

        await().pollInterval(500, MILLISECONDS)
            .atMost(5, SECONDS)
            .untilAsserted(() -> {
                //when
                Throwable throwable = catchThrowable(clusterHealth::check);

                //then
                assertThat(throwable).isNotNull();
                assertThat(throwable).isInstanceOf(ClusterUnhealthyException.class);
                assertThat(throwable).hasMessage("Cluster not healthy, the following hosts are down: [localhost/127.0.0.1]");
                throwable.printStackTrace();
            });
    }
}
