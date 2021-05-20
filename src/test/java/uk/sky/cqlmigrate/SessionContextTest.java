package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SessionContextTest {

    private SessionContext sessionContext;
    @Mock
    private CqlSession session;
    @Mock
    private ClusterHealth mockedClusterHealth;

    @Before
    public void setUp() {
        sessionContext = new SessionContext(session, ConsistencyLevel.ONE, ConsistencyLevel.TWO, mockedClusterHealth);
    }

    @Test
    public void shouldCallClusterHealth() {
        // when
        sessionContext.checkClusterHealth();
        // then
        Mockito.verify(mockedClusterHealth, Mockito.times(1)).check();
    }

    @Test
    public void shouldCallClusterHealthOnlyOnce() {
        // given
        sessionContext.checkClusterHealth();
        // when
        sessionContext.checkClusterHealth();
        // then
        Mockito.verify(mockedClusterHealth, Mockito.times(1)).check();
    }
}
