package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.*;

public class CqlMigratorHealthCheckTest {
    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static String username = "cassandra";
    private static String password = "cassandra";
    private static int binaryPort;
    private static Session session;
    private static final String TEST_KEYSPACE = "cqlmigrate_clusterhealth_test";

    private ClusterHealth mockClusterHealth = mock(ClusterHealth.class);
    private final SessionContextFactory sessionContextFactory = new SessionContextFactory() {
        @Override
        SessionContext getInstance(Session session, CqlMigratorConfig cqlMigratorConfig) {
            return new SessionContext(session, cqlMigratorConfig.getReadConsistencyLevel(), cqlMigratorConfig.getWriteConsistencyLevel(), mockClusterHealth);
        }
    };

    private final CqlMigratorImpl migrator = new CqlMigratorImpl(CqlMigratorConfig.builder()
            .withLockConfig(CassandraLockConfig.builder().build())
            .withReadConsistencyLevel(ConsistencyLevel.ALL)
            .withWriteConsistencyLevel(ConsistencyLevel.ALL)
            .build(), sessionContextFactory);

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        binaryPort = EmbeddedCassandraServerHelper.getNativeTransportPort();

        session = EmbeddedCassandraServerHelper.getSession();
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_clusterhealth_test");
        session.execute("CREATE KEYSPACE IF NOT EXISTS cqlmigrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS cqlmigrate.locks (name text PRIMARY KEY, client text)");
    }

    @After
    public void tearDown() {
        session.execute("TRUNCATE cqlmigrate.locks");
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        session.execute("DROP KEYSPACE cqlmigrate");
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void checkClusterHealthCheckedWhenBootstrappingCassandra() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_cluster_health_bootstrap"));
        //when
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        verify(mockClusterHealth, times(1)).check();
    }

    @Test
    public void checkClusterHealthCheckedWhenCreatingSchemaMigrateTable() throws Exception {
        //given
        session.execute("CREATE KEYSPACE cqlmigrate_clusterhealth_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_cluster_health_bootstrap"));
        //when
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        verify(mockClusterHealth, times(1)).check();
    }

    @Test
    public void checkClusterHealthCheckedWhenThereAreChanges() throws Exception {
        //given
        Collection<Path> cqlPaths = new ArrayList<>();
        cqlPaths.add(getResourcePath("cql_cluster_health_bootstrap"));
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        cqlPaths.add(getResourcePath("cql_cluster_health_first"));
        Mockito.reset(mockClusterHealth);
        //when
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        //then
        verify(mockClusterHealth, times(1)).check();
    }

    @Test
    public void checkClusterHealthCheckedWhenThereAreFollowingChanges() throws Exception {
        //given
        Collection<Path> cqlPaths = Lists.newArrayList(getResourcePath("cql_cluster_health_bootstrap"), getResourcePath("cql_cluster_health_first"));
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        Mockito.reset(mockClusterHealth);
        cqlPaths.add(getResourcePath("cql_cluster_health_second"));
        //when
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        //then
        verify(mockClusterHealth, times(1)).check();
    }

    @Test
    public void checkClusterHealthNotCheckedWhenThereAreNoChanges() throws Exception {
        Collection<Path> cqlPaths = Lists.newArrayList(getResourcePath("cql_cluster_health_bootstrap"), getResourcePath("cql_cluster_health_first"));
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        Mockito.reset(mockClusterHealth);
        //when
        migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        //then
        verify(mockClusterHealth, never()).check();
    }
}
