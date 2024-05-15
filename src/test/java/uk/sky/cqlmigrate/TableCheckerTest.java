package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import org.awaitility.core.ConditionTimeoutException;
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
import static org.assertj.core.api.Assertions.catchThrowable;


public class TableCheckerTest {

    private static final int defaultStartingPort = PortScavenger.getFreePort();
    private static final Server server = Server.builder().build();

    private static BoundCluster bCluster;
    private static CqlSession realCluster;

    private TableChecker tableChecker;

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

        tableChecker = new TableChecker(Duration.ofSeconds(5));
    }

    @AfterClass
    public static void tearDown() {
        realCluster.close();
        bCluster.close();
        server.close();
    }

    @Test
    public void shouldPass() {
        bCluster.prime(when("SELECT cluster_name FROM system.local WHERE key='local';")
                .then(rows().row("cluster_name", "Amazon Keyspaces").build()));

        bCluster.prime(when("SELECT table_name, status FROM system_schema_mcs.tables WHERE keyspace_name=?;")
                .then(rows().row("table_name", "test_table", "status", "ACTIVE").build()));

        Throwable throwable = catchThrowable(() -> tableChecker.check(realCluster, "test_keyspace"));
        assertThat(throwable).isNull();
    }

    @Test
    public void shouldThrowExceptionAfterWaitTime() {
        bCluster.prime(when("SELECT cluster_name FROM system.local WHERE key='local';")
                .then(rows().row("cluster_name", "Amazon Keyspaces").build()));

        bCluster.prime(when("SELECT table_name, status FROM system_schema_mcs.tables WHERE keyspace_name=?;")
                .then(rows().row("table_name", "test_table", "status", "UPDATE").build()));

        Throwable throwable = catchThrowable(() -> tableChecker.check(realCluster, "test_keyspace"));
        assertThat(throwable).isNotNull();
        assertThat(throwable).isInstanceOf(ConditionTimeoutException.class);
        assertThat(throwable).hasMessage("Condition with lambda expression in uk.sky.cqlmigrate.TableChecker was not fulfilled within 5 seconds.");
    }

    @Test
    public void shouldPassIfNotAwsk() {
        bCluster.prime(when("SELECT cluster_name FROM system.local WHERE key='local';")
                .then(rows().row("cluster_name", "0").build()));

        bCluster.prime(when("SELECT table_name, status FROM system_schema_mcs.tables WHERE keyspace_name=?;")
                .then(rows().row("table_name", "test_table", "status", "UPDATE").build()));

        Throwable throwable = catchThrowable(() -> tableChecker.check(realCluster, "test_keyspace"));
        assertThat(throwable).isNull();
    }
}
