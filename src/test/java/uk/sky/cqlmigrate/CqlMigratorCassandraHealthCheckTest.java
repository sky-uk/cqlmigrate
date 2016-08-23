package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CassandraClusterFactory.class, CqlMigratorImpl.class})
public class CqlMigratorCassandraHealthCheckTest {

    private static final String[] HOSTS = {"host167"};
    private static final int PORT = 7434;
    private static final String USERNAME = "user126";
    private static final String PASSWORD = "pass684";

    @Mock
    private CassandraClusterFactory clusterFactory;
    @Mock
    private Cluster cluster;
    @Mock
    private Session session;
    @Mock
    private ClusterHealth clusterHealthMock;

    @Before
    public void setUp() throws Exception {
        mockStatic(CassandraClusterFactory.class);
        when(CassandraClusterFactory.createCluster(HOSTS, PORT, USERNAME, PASSWORD)).thenReturn(cluster);
        when(cluster.connect()).thenReturn(session);
        when(session.getCluster()).thenReturn(cluster);

        PowerMockito.whenNew(ClusterHealth.class).withAnyArguments().thenReturn(clusterHealthMock);
        PowerMockito.whenNew(CassandraLockingMechanism.class).withAnyArguments().thenThrow(new SuspendCodeExecutionException("Provided to suspend code execution for the testcase."));
    }

    @Test
    public void shouldCheckClusterHealthWhenConfigIsOn() throws Exception {
        // given
        boolean checkAllNodesHealthy = true;

        CqlMigratorConfig config = getCqlMigratorConfig(checkAllNodesHealthy);
        CqlMigratorImpl cqlMigrator = new CqlMigratorImpl(config);
        // when
        try {
            cqlMigrator.migrate(HOSTS, PORT, USERNAME, PASSWORD, null, Collections.emptyList());
        } catch (SuspendCodeExecutionException e) {
            // expected from PowerMock
        }
        //then
        verify(clusterHealthMock).check();
    }

    @Test
    public void shouldNotCheckClusterHealthWhenConfigIsOff() throws Exception {
        // given
        boolean checkAllNodesHealthy = false;

        CqlMigratorConfig config = getCqlMigratorConfig(checkAllNodesHealthy);
        CqlMigratorImpl cqlMigrator = new CqlMigratorImpl(config);
        // when
        try {
            cqlMigrator.migrate(HOSTS, PORT, USERNAME, PASSWORD, null, Collections.emptyList());
        } catch (SuspendCodeExecutionException e) {
            // expected from PowerMock
        }
        //then
        verify(clusterHealthMock, never()).check();
    }

    private CqlMigratorConfig getCqlMigratorConfig(boolean checkAllNodesHealthy) {
        return CqlMigratorConfig.builder()
                .withReadConsistencyLevel(ConsistencyLevel.ONE)
                .withWriteConsistencyLevel(ConsistencyLevel.TWO)
                .withCassandraLockConfig(CassandraLockConfig.builder().build())
                .withCheckAllNodesHealthy(checkAllNodesHealthy)
                .build();
    }

    private static class SuspendCodeExecutionException extends RuntimeException {
        public SuspendCodeExecutionException(String message) {
            super(message);
        }
    }
}
