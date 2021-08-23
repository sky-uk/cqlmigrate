package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

@Ignore("Ignored till we can move it out of pipeline build")
public class LockVerificationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockVerificationTest.class);
    private static final String CASSANDRA_HOST = "localhost";

    private static final int CASSANDRA_PORT = 9042;
    private static final String KEYSPACE = "locker";
    private static String TABLE_NAME = KEYSPACE + ".lock_testing";

    private static Server server = Server.builder().build();
    private ClusterSpec cluster = ClusterSpec.builder().build();
    private BoundCluster bCluster;
    private CqlSession session;
    private DataCenterSpec dc;
    DriverConfigLoader loader;

    @Before
    public void setUp() throws UnknownHostException {
        dc = cluster.addDataCenter().withName("DC1").withCassandraVersion("3.8").build();
        dc.addNode().withAddress(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), CASSANDRA_PORT)).build();
        dc.addNode().withPeerInfo("host_id", UUID.randomUUID()).build();
        bCluster = server.register(cluster);

        loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(1000))
                .endProfile()
                .build();

        session = CqlSession.builder()
                .withConfigLoader(loader)
                .addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), CASSANDRA_PORT))
                .withLocalDatacenter(dc.getName()).build();

        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate");
        session.execute("CREATE KEYSPACE IF NOT EXISTS cqlmigrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);
        session.execute("CREATE TABLE IF NOT EXISTS cqlmigrate.locks (name text PRIMARY KEY, client text);");
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);
    }

    @After
    public void cleanUp() {
        session.execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate");
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        session.close();
    }

    @Test
    public void shouldManageContentionsForSchemaMigrate() throws InterruptedException, URISyntaxException {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);

        final Collection<Path> cqlPaths = singletonList(Paths.get(ClassLoader.getSystemResource("cql_migrate_multithreads").toURI()));
        final Callable<String> cqlMigrate = () -> {
            CqlSession session = CqlSession.builder()
                    .withConfigLoader(loader)
                    .addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), CASSANDRA_PORT))
                    .withLocalDatacenter(dc.getName()).build();

            CqlMigrator cqlMigrator = CqlMigratorFactory.create(CassandraLockConfig.builder().build());
            cqlMigrator.migrate(session, "cqlmigrate_test", cqlPaths);

            session.close();
            return "Done";
        };

        List<Callable<String>> workers = IntStream.range(0, 25)
                .mapToObj(i -> cqlMigrate)
                .collect(Collectors.toList());

        final ExecutorService threadPool = newFixedThreadPool(25);
        final List<Future<String>> futures = threadPool.invokeAll(workers);

        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });

        threadPool.shutdown();
    }

    @Test
    public void shouldObtainLockToInsertRecord() throws InterruptedException {
        session.execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", KEYSPACE));
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);
        session.execute(String.format("CREATE TABLE %s (id text PRIMARY KEY, counter int);", TABLE_NAME));
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);
        session.execute(SimpleStatement.newInstance(String.format("INSERT INTO %s (id, counter) VALUES (?, ?);", TABLE_NAME), "lock-tester", 0).setConsistencyLevel(ConsistencyLevel.QUORUM));

        final int maximumCounter = 1000;
        final int maximumWorkers = 25;
        final Callable<List<Integer>> worker = () -> {
            CqlSession session = CqlSession.builder()
                    .withConfigLoader(loader)
                    .addContactPoint(new InetSocketAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}), CASSANDRA_PORT))
                    .withLocalDatacenter(dc.getName()).build();

            CassandraLockingMechanism lockingMechanism = new CassandraLockingMechanism(session, KEYSPACE, ConsistencyLevel.ALL, "cqlmigrate");
            CassandraLockConfig lockConfig = createCassandraLockConfig();

            Lock lock = new Lock(lockingMechanism, lockConfig);

            boolean done = false;
            List<Integer> counters = new ArrayList<>();
            do {
                lock.lock();

                int currentCounter = readCurrentCounter();
                done = currentCounter >= maximumCounter;
                if (!done) {
                    int updatedCounter = currentCounter + 1;
                    updateCounter(updatedCounter);
                    counters.add(updatedCounter);
                    LOGGER.info("Client {} registered counter {}", lockConfig.getClientId(), updatedCounter);
                }

                lock.unlock(false);
            } while (!done);

            session.close();
            return counters;
        };

        List<Callable<List<Integer>>> workers = IntStream.range(0, maximumWorkers)
                .mapToObj(i -> worker)
                .collect(Collectors.toList());

        final ExecutorService threadPool = newFixedThreadPool(maximumWorkers);
        final List<Future<List<Integer>>> futures = threadPool.invokeAll(workers);

        Set<Integer> allCounters = new HashSet<>();

        futures.forEach(future -> {
            try {
                List<Integer> workerCounters = future.get();
                for (int counter : workerCounters) {
                    assertThat(allCounters.add(counter)).as("Locking failed: duplicate counter was found: " + counter).isTrue();
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });

        assertThat(allCounters.size()).isEqualTo(maximumCounter);

        threadPool.shutdown();
    }

    private void updateCounter(int updatedCounter) {
        Statement updateCounterQuery = SimpleStatement.newInstance(String.format("UPDATE %s SET counter = %d WHERE id = 'lock-tester'", TABLE_NAME, updatedCounter)).setConsistencyLevel(ConsistencyLevel.QUORUM);
        session.execute(updateCounterQuery);

    }

    private int readCurrentCounter() {
        Statement readCounterQuery = SimpleStatement.newInstance(String.format("SELECT counter from %s WHERE id = 'lock-tester'", TABLE_NAME)).setConsistencyLevel(ConsistencyLevel.QUORUM);
        final ResultSet currentRecord = session.execute(readCounterQuery);
        return currentRecord.one().getInt("counter");
    }

    private CassandraLockConfig createCassandraLockConfig() {
        return CassandraLockConfig.builder()
                .withPollingInterval(Duration.ofMillis(30)).build();
    }
}
