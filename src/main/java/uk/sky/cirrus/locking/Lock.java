package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.ConsistencyLevel.QUORUM;

public class Lock {

    private static final Logger log = LoggerFactory.getLogger(Lock.class);
    private static final UUID CLIENT = UUID.randomUUID();

    private final String name;
    private final Session session;

    private Lock(String name, Session session) {
        this.name = name;
        this.session = session;
    }

    public static Lock acquire(LockConfig lockConfig, String keyspace, Session session) {

        Duration pollingInterval = lockConfig.getPollingInterval();
        Duration timeout = lockConfig.getTimeout();

        ensureLocksSchemaExists(lockConfig, session);

        String name = keyspace + ".schema_migration";
        Statement query = new SimpleStatement("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS", name, CLIENT)
                .setConsistencyLevel(QUORUM);

        long startTime = System.currentTimeMillis();

        while (true) {

            ResultSet resultSet;
            try {
                resultSet = session.execute(query);
            } catch (DriverException e) {
                log.warn("Query to acquire lock failed to execute", e);
                throw new CannotAcquireLockException("Query failed to execute", e);
            }

            Row lock = resultSet.one();
            boolean lockAcquired = lock.getBool("[applied]");

            if (lockAcquired) {
                log.debug("Lock acquired for: {}", name);
                return new Lock(name, session);
            } else {
                UUID clientWithLock = lock.getUUID("client");
                log.debug("Lock currently in use by client: {}", clientWithLock);
                Uninterruptibles.sleepUninterruptibly(pollingInterval.getMillis(), TimeUnit.MILLISECONDS);

                if (timedOut(timeout, startTime)) {
                    log.warn("Unable to acquire lock, currently in use by client: {}", clientWithLock);
                    throw new CannotAcquireLockException("Lock currently in use by client: " + clientWithLock);
                }
            }
        }

    }

    private static void ensureLocksSchemaExists(LockConfig lockConfig, Session session) {
        try {
            Statement query = new SimpleStatement(String.format(
                    "CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {'class': '%s' , 'replication_factor': %s}",
                    lockConfig.getReplicationClass(),
                    lockConfig.getReplicationFactor()
            ));
            session.execute(query);

            query = new SimpleStatement("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)");
            session.execute(query);
        } catch (DriverException e) {
            log.warn("Query to create locks keyspace or locks table failed to execute", e);
            throw new CannotAcquireLockException("Query to create locks schema failed to execute", e);
        }
    }

    private static boolean timedOut(Duration timeout, long startTime) {
        long currentDuration = System.currentTimeMillis() - startTime;
        return currentDuration >= timeout.getMillis();
    }

    public void release() {
        Statement query = new SimpleStatement("DELETE FROM locks.locks WHERE name = ?", name)
                .setConsistencyLevel(QUORUM);

        try {
            session.execute(query);
            log.debug("Lock released for: {}", name);
        } catch (DriverException e) {
            log.warn("Query to release lock failed to execute", e);
            throw new CannotReleaseLockException("Query failed to execute", e);
        }
    }
}
