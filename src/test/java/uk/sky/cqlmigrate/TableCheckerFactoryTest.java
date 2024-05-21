package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.sky.cqlmigrate.util.PortScavenger;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

public class TableCheckerFactoryTest {

    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static final Server server = Server.builder().build();
    public static final CqlMigratorConfig CQL_MIGRATOR_CONFIG = CqlMigratorConfig.builder()
            .withLockConfig(LockConfig.builder().build())
            .withReadConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
            .withWriteConsistencyLevel(ConsistencyLevel.ALL)
            .withTableCheckerTimeout(Duration.ofMinutes(1))
            .build();

    private static BoundCluster bCluster;
    private static CqlSession realCluster;

    private TableCheckerFactory tableCheckerFactory;

    @BeforeClass
    public static void classSetup() throws UnknownHostException {
        ClusterSpec cluster = ClusterSpec.builder().build();
        DataCenterSpec dc = cluster.addDataCenter().withName("DC1").withCassandraVersion("3.11").build();
        dc.addNode().withAddress(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort)).build();
        dc.addNode().withPeerInfo("host_id", UUID.randomUUID()).build();
        bCluster = server.register(cluster);

        realCluster = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), defaultStartingPort))
                .withLocalDatacenter(dc.getName())
                .build();
    }

    @Before
    public void baseSetup() {
        bCluster.clearPrimes(true);
        bCluster.clearLogs();

        tableCheckerFactory = new TableCheckerFactory();
    }

    @AfterClass
    public static void tearDown() {
        realCluster.close();
        bCluster.close();
        server.close();
    }

    @Test
    public void shouldReturnAwksImpl() {
        bCluster.prime(when("SELECT cluster_name FROM system.local WHERE key='local';")
                .then(rows().row("cluster_name", "Amazon Keyspaces").build()));

        TableChecker instance = tableCheckerFactory.getInstance(realCluster, CQL_MIGRATOR_CONFIG);

        assertThat(instance).isInstanceOf(AwskTableChecker.class);
    }

    @Test
    public void shouldReturnNoopImpl() {
        bCluster.prime(when("SELECT cluster_name FROM system.local WHERE key='local';")
                .then(rows().row("cluster_name", "random cluster name").build()));

        TableChecker instance = tableCheckerFactory.getInstance(realCluster, CQL_MIGRATOR_CONFIG);

        assertThat(instance).isInstanceOf(NoOpTableChecker.class);
    }
}
