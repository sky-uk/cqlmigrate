package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

public class LockVerificationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockVerificationTest.class);
    private static final BlockingQueue<Optional<Integer>> CASSANDRA_OPERATION = new ArrayBlockingQueue<>(25);
    private static final String CASSANDRA_HOST = "localhost";

    private static final int CASSANDRA_PORT = 9042;
    private static final String KEYSPACE = "locker";
    private static String TABLE_NAME = KEYSPACE + "." + "lock_testing";

    private static final Statement SELECT_STATEMENT = new SimpleStatement(String.format("SELECT counter from %s WHERE id = 'lock-tester'", TABLE_NAME)).setConsistencyLevel(ConsistencyLevel.QUORUM);

    private Cluster cluster;
    private Session session;

    @Before
    public void setUp() {
        cluster = createCluster();
        session = cluster.connect();

        session.execute(String.format("DROP KEYSPACE IF EXISTS %s;", KEYSPACE));
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", KEYSPACE));
        session.execute(String.format("CREATE TABLE %s (id text PRIMARY KEY, counter int);", TABLE_NAME));
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
    public void shouldObtainLockToInsertRecord() throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(25);
        final LockConfig config = LockConfig.builder().withPollingInterval(Duration.ofMillis(30)).withSimpleStrategyReplication(3).build();

        final Set<Integer> counters = new HashSet<>();

        final Callable<String> worker = () -> {
            UUID client = UUID.randomUUID();
            LOGGER.info("UUID: {}", client);
            Optional<Integer> counter = Optional.absent();
            try {

                final Lock lock = Lock.acquire(config, KEYSPACE, session, client);
                counter = readAndIncrementColumnValue();
                lock.release();

            } catch (Exception e) {
                LOGGER.error("Unexpected exception:", e);
            } finally {
                CASSANDRA_OPERATION.put(counter);
            }

            return "Done";
        };

        for (int i = 0; i < 8; i++) {
            executorService.submit(worker);
        }


        spawnWorkerOnDemand(executorService, counters, worker);

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.MINUTES);
    }

    private void spawnWorkerOnDemand(ExecutorService executorService, Set<Integer> counters, Callable<String> worker) throws InterruptedException {
        int counter = 1;
        int maximum_iterations = 1000000;

        while (counter < maximum_iterations) {
            Optional<Integer> polledValue = CASSANDRA_OPERATION.take();
            counter = polledValue.or(counter);
            if (polledValue.isPresent()) {
                if (!counters.add(polledValue.get())) {
                    Assert.fail("Lock allowed a duplicate counter: " + polledValue);
                }
                executorService.submit(worker);
            }
        }
    }

    private Optional<Integer> readAndIncrementColumnValue() {
        ResultSet currentRecord = session.execute(SELECT_STATEMENT);
        int counter = currentRecord.one().getInt("counter");

        LOGGER.info("Counter Value: {}", counter);
        session.execute(new SimpleStatement(String.format("UPDATE %s SET counter = %d WHERE id = 'lock-tester'", TABLE_NAME, counter + 1)).setConsistencyLevel(ConsistencyLevel.QUORUM));
        return Optional.of(counter);
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
}