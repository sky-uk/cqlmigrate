package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionContextTest {

    private SessionContext sessionContext;

    @Mock
    private Session session;
    @Mock
    private Cluster cluster;
    @Mock
    private ClusterHealth mockedClusterHealth;

    @Before
    public void setUp() throws Exception {
        Mockito.when(session.getCluster()).thenReturn(cluster);
        sessionContext = new SessionContext(session, ConsistencyLevel.ONE, ConsistencyLevel.TWO, mockedClusterHealth);
    }


    @Test
    public void shouldCallClusterHealth() throws Exception {
        // when
        sessionContext.checkClusterHealth();
        // then
        Mockito.verify(mockedClusterHealth, Mockito.times(1)).check();
    }

    @Test
    public void shouldCallClusterHealthOnlyOnce() throws Exception {
        // given
        sessionContext.checkClusterHealth();
        // when
        sessionContext.checkClusterHealth();
        // then
        Mockito.verify(mockedClusterHealth, Mockito.times(1)).check();
    }
}