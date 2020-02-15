package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.server.AddressResolver;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Inet4Resolver;
import com.datastax.oss.simulacron.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;

import static com.datastax.oss.simulacron.common.stubbing.CloseType.DISCONNECT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

public class ClusterHealthTest {


    private static CqlSession session;
    private ClusterHealth clusterHealth;

    private static Server server = Server.builder().build();
    private ClusterSpec cluster = ClusterSpec.builder().build();
    private BoundCluster bCluster;
    private static final byte[] defaultStartingIp = new byte[] {127, 0, 0, 1};
    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static ClusterSpec clusterSpec;
    AddressResolver addressResolver ;
    SocketAddress availableAddress;

    @Before
    public void setUp() {
        DataCenterSpec dc = cluster.addDataCenter().withCassandraVersion("3.8").build();
        addressResolver = new Inet4Resolver(defaultStartingIp, defaultStartingPort);
        availableAddress = addressResolver.get();
        dc.addNode().withAddress(availableAddress).build();
        dc.addNode().withPeerInfo("host_id", UUID.randomUUID()).build();
        bCluster = server.register(cluster);
        session = CqlSession.builder().addContactPoint((InetSocketAddress) availableAddress).withLocalDatacenter(dc.getName()).build();
        clusterHealth = new ClusterHealth(session);
    }

    @After
    public void tearDown() {
        addressResolver.release(availableAddress);
        server.close();
    }

    @Test
    public void shouldThrowExceptionIfHostIsDown() {
        //given
        bCluster.node(0).closeConnections(DISCONNECT);

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
