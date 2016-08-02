package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ClusterHealthTest {

    private static final int BINARY_PORT = PortScavenger.getFreePort();
    private static final int ADMIN_PORT = PortScavenger.getFreePort();
    private static String username = "cassandra";
    private static String password = "cassandra";
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
                .withCredentials(username, password)
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
    public void shouldThrowExceptionIfHostIsDown() throws Exception {
        //given
        scassandra.stop();

        //when
        Throwable throwable = catchThrowable(clusterHealth::check);

        //then
        throwable.printStackTrace();
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(ClusterUnhealthyException.class);
        assertThat(throwable).hasMessage("Cluster not healthy, the following hosts are down: [localhost/127.0.0.1]");
    }
}
