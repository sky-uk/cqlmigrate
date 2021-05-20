package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.RejectScope;
import com.datastax.oss.simulacron.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static com.datastax.oss.simulacron.common.stubbing.CloseType.DISCONNECT;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

public class ClusterHealthTest {

    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static final Server server = Server.builder().build();

    private final ClusterSpec cluster = ClusterSpec.builder().build();

    private ClusterHealth clusterHealth;
    private BoundCluster bCluster;
    private CqlSession realCluster;

    @Before
    public void setUp() throws UnknownHostException {
        DataCenterSpec dc = cluster.addDataCenter().withName("DC1").withCassandraVersion("3.11").build();
        dc.addNode().withAddress(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort)).build();
        dc.addNode().withPeerInfo("host_id", UUID.randomUUID()).build();
        bCluster = server.register(cluster);

        bCluster.prime(when("select cluster_name from system.local where key = 'local'")
                .then(rows().row("cluster_name", "0").build()));

        realCluster = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort))
                .withLocalDatacenter(dc.getName())
                .build();
        clusterHealth = new ClusterHealth(realCluster);
    }

    @After
    public void tearDown() {
        realCluster.close();
    }

    @Test
    public void shouldThrowExceptionIfHostIsDown() {
        //given
        bCluster.node(0).closeConnections(DISCONNECT);
        bCluster.node(0).rejectConnections(0, RejectScope.UNBIND);

        await().pollInterval(500, MILLISECONDS)
                .atMost(5, SECONDS)
                .untilAsserted(() -> {
                    //when
                    Throwable throwable = catchThrowable(clusterHealth::check);

                    //then
                    assertThat(throwable).isNotNull();
                    assertThat(throwable).isInstanceOf(ClusterUnhealthyException.class);
                    assertThat(throwable).hasMessage("Cluster not healthy, the following hosts are down: [/127.0.0.1]");
                    throwable.printStackTrace();
                });
    }
}
