//package uk.sky.cqlmigrate;
//
//import com.datastax.driver.core.Cluster;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.ArgumentMatcher;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyInt;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.verify;
//import static org.powermock.api.mockito.PowerMockito.mockStatic;
//import static org.powermock.api.mockito.PowerMockito.when;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({Cluster.class})
//public class CassandraClusterFactoryTest {
//
//    private static class VarArgMatcher implements ArgumentMatcher<String[]> {
//        @Override
//        public boolean matches(final String[] argument) {
//            return true;
//        }
//    }
//
//    @Mock
//    private Cluster.Builder clusterBuilderMock;
//
//    @Before
//    public void setUp() throws Exception {
//        mockStatic(Cluster.class);
//        when(Cluster.builder()).thenReturn(clusterBuilderMock);
//        when(clusterBuilderMock.addContactPoints(Mockito.<String[]>any())).thenReturn(clusterBuilderMock);
//        when(clusterBuilderMock.withPort(anyInt())).thenReturn(clusterBuilderMock);
//        when(clusterBuilderMock.withCredentials(any(String.class), any(String.class))).thenReturn(clusterBuilderMock);
//    }
//
//    @Test
//    public void createClusterWithCredentials() throws Exception {
//        String expectedUser = "cassandra-user";
//        String expectedPassword = "cassandra-password";
//
//        //when
//        CassandraClusterFactory.createCluster(new String[]{"host1"}, 0, expectedUser, expectedPassword);
//
//        //then
//        verify(clusterBuilderMock).withCredentials(expectedUser, expectedPassword);
//    }
//
//    @Test
//    public void createClusterWithNullCredentials() throws Exception {
//        String expectedUser = null;
//        String expectedPassword = null;
//
//        //when
//        CassandraClusterFactory.createCluster(new String[]{"host1"}, 0, expectedUser, expectedPassword);
//
//        //then
//        verify(clusterBuilderMock, never()).withCredentials(expectedUser, expectedPassword);
//    }
//
//
//    @Test
//    public void createClusterWithNullPassword() throws Exception {
//        String expectedUser = "cassandra";
//        String expectedPassword = null;
//
//        //when
//        CassandraClusterFactory.createCluster(new String[]{"host1"}, 0, expectedUser, expectedPassword);
//
//        //then
//        verify(clusterBuilderMock, never()).withCredentials(expectedUser, expectedPassword);
//    }
//
//    @Test
//    public void createClusterWithNullUser() throws Exception {
//        String expectedUser = null;
//        String expectedPassword = "cassandra";
//
//        //when
//        CassandraClusterFactory.createCluster(new String[]{"host1"}, 0, expectedUser, expectedPassword);
//
//        //then
//        verify(clusterBuilderMock, never()).withCredentials(expectedUser, expectedPassword);
//    }
//}
