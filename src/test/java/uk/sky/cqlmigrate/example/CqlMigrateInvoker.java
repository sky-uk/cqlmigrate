package uk.sky.cqlmigrate.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import uk.sky.cqlmigrate.CassandraLockConfig;
import uk.sky.cqlmigrate.CqlMigrator;
import uk.sky.cqlmigrate.CqlMigratorFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMillis;

public class CqlMigrateInvoker {

    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static int binaryPort;
    private static String username = "cassandra";
    private static String password = "cassandra";
    private static Cluster cluster;
    private static Session session;
    private static final String TEST_KEYSPACE = "cqlmigrate_test";

    public void doMigrate() throws IOException, URISyntaxException {
        CqlMigrator cqlMigrator = CqlMigratorFactory.create(CassandraLockConfig.builder().withTimeout(ofMillis(300)).build());

        URL resource = this.getClass().getResource("");
        try (FileSystem fs = FileSystems.newFileSystem(resource.toURI(), ImmutableMap.of("create", "true"))) {
            List<Path> resourcePaths = Lists.newArrayList(fs.getPath("cql_bootstrap"));
            cqlMigrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, resourcePaths);
        }
    }

    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        binaryPort = EmbeddedCassandraServerHelper.getNativeTransportPort();
        cluster = EmbeddedCassandraServerHelper.getCluster();
        session = EmbeddedCassandraServerHelper.getSession();
    }

    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS " + TEST_KEYSPACE);
        session.execute("CREATE KEYSPACE IF NOT EXISTS cqlmigrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS cqlmigrate.locks (name text PRIMARY KEY, client text)");
    }

    public void tearDown() {
        session.execute("TRUNCATE cqlmigrate.locks");
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
        session.execute("DROP KEYSPACE cqlmigrate");
    }

    public static void tearDownCassandra() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }
}
