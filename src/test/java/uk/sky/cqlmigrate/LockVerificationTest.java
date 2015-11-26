package uk.sky.cqlmigrate;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import org.hamcrest.Matchers;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertThat;

@Ignore("Ignored till we can move it out of pipeline build")
public class LockVerificationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockVerificationTest.class);
    private static final BlockingQueue<Optional<Integer>> RESULTS_STORE = new ArrayBlockingQueue<>(25);
    private static final String CASSANDRA_HOST = "localhost";

    private static final int CASSANDRA_PORT = 9042;
    private static final String KEYSPACE = "locker";
    private static String TABLE_NAME = KEYSPACE + "." + "lock_testing";

    private Cluster cluster;
    private Session session;

    @Before
    public void setUp() {
        cluster = createCluster();
        session = cluster.connect();

        session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", KEYSPACE));
        session.execute("DROP KEYSPACE IF EXISTS locks");

        session.execute("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client text)");

        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", KEYSPACE));
        Uninterruptibles.sleepUninterruptibly(300, TimeUnit.MILLISECONDS);
        session.execute(String.format("CREATE TABLE %s (id text PRIMARY KEY, counter int);", TABLE_NAME));
        Uninterruptibles.sleepUninterruptibly(800, TimeUnit.MILLISECONDS);
        session.execute(new SimpleStatement(String.format("INSERT INTO %s (id, counter) VALUES (?, ?);", TABLE_NAME), "lock-tester", 1).setConsistencyLevel(ConsistencyLevel.QUORUM));
    }

    @After
    public void cleanUp() {
        final String dropKeyspaceQuery = String.format("DROP KEYSPACE IF EXISTS %s;", KEYSPACE);
        session.execute(dropKeyspaceQuery);
        session.close();
        cluster.close();
    }

    @Test
    public void shouldManageContentionsForSchemaMigrate() throws InterruptedException, URISyntaxException {

        final Path cql_bootstrap = Paths.get(ClassLoader.getSystemResource("cql_migrate_multithreads").toURI());
        final CqlMigratorImpl cqlMigrator = new CqlMigratorImpl(CassandraLockConfig.builder().build());

        final Collection<Path> cqlPaths = singletonList(cql_bootstrap);
        final ExecutorService cqlMigratorManager = newFixedThreadPool(25);
        final Callable<String> cqlMigrate = () ->  { cqlMigrator.migrate(session, "cqlmigrate_test", cqlPaths); return "Done"; };
        final List<Callable<String>> workers = newArrayList();

        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test;");

        for(int i=0; i< 20; i++){
            workers.add(cqlMigrate);
        }

        Stopwatch executionTime = Stopwatch.createStarted();
        final List<Future<String>> futures = cqlMigratorManager.invokeAll(workers);

        System.out.println(futures.size());

        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        });

        executionTime.stop();

        cqlMigratorManager.shutdown();

        final long elapsed = executionTime.elapsed(TimeUnit.SECONDS);
        assertThat("Schema migration took longer than 25s", elapsed, Matchers.lessThanOrEqualTo(25L));
    }

    @Test
    public void shouldObtainLockToInsertRecord() throws InterruptedException {
        final CassandraLockConfig config = CassandraLockConfig.builder()
                .withPollingInterval(Duration.ofMillis(30))
                .withSimpleStrategyReplication(3)
                .build();

        final Callable<Optional<Integer>> worker = () -> {
            Integer counter = null;
            try {
                UUID clientId = UUID.randomUUID();
                CassandraLockingMechanism lockingMechanism = new CassandraLockingMechanism(session, KEYSPACE, config);
                final Lock lock = Lock.acquire(lockingMechanism, config);
                counter = readAndIncrementCounter();
                LOGGER.info("Client {} registered counter {}", clientId, counter);
                lock.release();

            } catch (Exception e) {
                LOGGER.error("Unexpected exception:", e);
            } finally {
                RESULTS_STORE.put(Optional.fromNullable(counter));
            }
            return Optional.fromNullable(counter);
        };

        final LockVerifier lockVerifier = new LockVerifier(worker)
                .withMaximumParallelWorkers(4)
                .withExpectedIterations(100);
        lockVerifier.start();
        lockVerifier.validateProgressAndWaitUntilDone();

    }

    private int readAndIncrementCounter() {
        final int currentCounter = readCurrentCounter();
        final int updatedCounter = currentCounter + 1;
        session.execute(new SimpleStatement(String.format("UPDATE %s SET counter = %d WHERE id = 'lock-tester'", TABLE_NAME, updatedCounter)).setConsistencyLevel(ConsistencyLevel.QUORUM));

        return updatedCounter;
    }

    private int readCurrentCounter() {
        final ResultSet currentRecord = session.execute(new SimpleStatement(String.format("SELECT counter from %s WHERE id = 'lock-tester'", TABLE_NAME)).setConsistencyLevel(ConsistencyLevel.QUORUM));
        return currentRecord.one().getInt("counter");
    }

    private Cluster createCluster() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        return Cluster.builder()
                .addContactPoints(CASSANDRA_HOST)
                .withPort(CASSANDRA_PORT)
                .withQueryOptions(queryOptions)
                .build();
    }

    private static class LockVerifier {
        private final ExecutorService executorService;
        private int maximumConcurrentThreads;
        private int maximumIterations;
        private final Callable<Optional<Integer>> worker;
        private final Set<Integer> results;

        private LockVerifier(Callable<Optional<Integer>> worker) {
            this.maximumConcurrentThreads = 4;
            this.maximumIterations = 10_000_000;
            this.worker = worker;

            this.executorService = newFixedThreadPool(25);
            this.results = new HashSet<>();
        }

        public LockVerifier withMaximumParallelWorkers(int maximumParallelWorkers) {
            this.maximumConcurrentThreads = maximumParallelWorkers;
            return this;
        }

        public LockVerifier withExpectedIterations(int maximumIterations) {
            this.maximumIterations = maximumIterations;
            return this;
        }

        public void start() {
            for (int i = 0; i < maximumConcurrentThreads; i++) {
                executorService.submit(worker);
            }
        }

        public void validateProgressAndWaitUntilDone() throws InterruptedException {
            int counter = 1;
            while (counter < maximumIterations) {
                final Optional<Integer> polledValue = RESULTS_STORE.take();
                counter = polledValue.or(counter);

                if (polledValue.isPresent()) {
                    if (!results.add(polledValue.get())) {
                        Assert.fail("Lock allowed a duplicate counter: " + polledValue);
                    }
                    LOGGER.info("Current counter {}", counter);
                    executorService.submit(worker);
                }
            }
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        }
    }
}
