package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CqlSession.class})
public class CassandraClusterFactoryTest {

    private static class VarArgMatcher implements ArgumentMatcher<String[]> {
        @Override
        public boolean matches(final String[] argument) {
            return true;
        }
    }

    @Mock
    private CqlSession session;

    @Mock
    private CqlSessionBuilder cqlSessionBuilder;

    @Before
    public void setUp() throws Exception {
        mockStatic(CqlSession.class);

        when(CqlSession.builder()).thenReturn(cqlSessionBuilder);
        when(cqlSessionBuilder.addContactPoints(anyCollection())).thenReturn(cqlSessionBuilder);
        when(cqlSessionBuilder.withAuthCredentials(any(String.class), any(String.class))).thenReturn(cqlSessionBuilder);
        when(cqlSessionBuilder.build()).thenReturn(session);

    }

    @Test
    public void createClusterWithCredentials() throws Exception {
        String expectedUser = "cassandra-user";
        String expectedPassword = "cassandra-password";

        //when
        CassandraClusterFactory.createCluster(new String[] {"host1"}, 0, expectedUser, expectedPassword);
        InetSocketAddress testInetSocketAddress = new InetSocketAddress("host1", 0);

        //then
        verify(cqlSessionBuilder).addContactPoints(Collections.singletonList(testInetSocketAddress));
        verify(cqlSessionBuilder).withAuthCredentials(expectedUser, expectedPassword);
    }

    @Test
    public void createClusterWithNullCredentials() throws Exception {
        String expectedUser = null;
        String expectedPassword = null;

        //when
        CassandraClusterFactory.createCluster(new String[] {"host1"}, 0, expectedUser, expectedPassword);

        //then
        verify(cqlSessionBuilder, never()).withAuthCredentials(expectedUser, expectedPassword);
    }


    @Test
    public void createClusterWithNullPassword() throws Exception {
        String expectedUser = "cassandra";
        String expectedPassword = null;

        //when
        CassandraClusterFactory.createCluster(new String[] {"host1"}, 0, expectedUser, expectedPassword);

        //then
        verify(cqlSessionBuilder, never()).withAuthCredentials(expectedUser, expectedPassword);
    }

    @Test
    public void createClusterWithNullUser() throws Exception {
        String expectedUser = null;
        String expectedPassword = "cassandra";

        //when
        CassandraClusterFactory.createCluster(new String[] {"host1"}, 0, expectedUser, expectedPassword);

        //then
        verify(cqlSessionBuilder, never()).withAuthCredentials(expectedUser, expectedPassword);
    }
}
