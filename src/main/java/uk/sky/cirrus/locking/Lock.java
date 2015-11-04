package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.time.Duration;
import java.util.UUID;

/**
 * Class to acquire a lock on cassandra table for a single application instance.  Each instance is given a unique identifier
 * and attempts to gain a lock on the table.  If no lock is currently acquired, the instance is given the lock on the table, otherwise
 * the instance must wait until the lock has been relinquished. If a lock cannot be acquired within the configured timeout
 * interval, an exception is thrown.
 *
 */
public class Lock {

    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private final String name;
    private final Session session;
    private final UUID client;

    private Lock(String name, Session session, UUID client) {
        this.name = name;
        this.session = session;
        this.client = client;
    }

    /**
     * @param lockConfig  {@code LockConfig} for configuring the lock to migrate the schema
     * @param keyspace  Name of the keyspace which is to be used for migration
     * @param session  Cassandra {@code Session} to use while acquiring the lock
     * @param client Identifier for the owner of the lock
     * @return the {@code Lock} object
     *
     * @throws CannotAcquireLockException if instance cannot acquire lock within the specified time interval or execution of query to insert lock fails
     */
    public static Lock acquire(LockConfig lockConfig, String keyspace, Session session, UUID client) {
        Duration pollingInterval = lockConfig.getPollingInterval();
        Duration timeout = lockConfig.getTimeout();
        int acquireAttempts = 1;

        ensureLocksSchemaExists(lockConfig, session);

        String name = keyspace + ".schema_migration";

        long startTime = System.currentTimeMillis();
        log.info("Attempting to acquire lock for '{}', using client id '{}'", name, client);

        while (true) {
            Statement query = new SimpleStatement("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS", name, client);

            ResultSet resultSet;
            try {
                resultSet = session.execute(query);
            } catch (DriverException e) {
                log.warn("Query to acquire lock for {} failed to execute", client, e);
                throw new CannotAcquireLockException("Query failed to execute", e);
            }

            Row currentLock = resultSet.one();
            boolean lockAcquired = currentLock.getBool("[applied]");

            if (lockAcquired) {
                log.info("Lock acquired for '{}' by client '{}' after {} attempts", name, client, acquireAttempts);
                return new Lock(name, session, client);
            } else {
                UUID clientWithLock = currentLock.getUUID("client");
                log.debug("Lock currently in use by client: {}", clientWithLock);

                if (timedOut(timeout, startTime)) {
                    log.warn("Unable to acquire lock for {} after {} attempts, currently in use by client: {}, time tried: {}", client, acquireAttempts, clientWithLock, (System.currentTimeMillis() - startTime));
                    throw new CannotAcquireLockException("Lock currently in use by client: " + clientWithLock);
                }

                try {
                    Thread.sleep(pollingInterval.toMillis());
                } catch (InterruptedException e) {
                    log.debug("Lock {} was interrupted", client);
                    Thread.currentThread().interrupt();
                }
            }
            acquireAttempts++;
        }

    }

    /**
     * @throws CannotReleaseLockException if execution of query to remove lock fails
     */
    public void release() {

        long timestamp = System.currentTimeMillis() * 1000;
        Statement query = new SimpleStatement("DELETE FROM locks.locks WHERE name = ? IF client = ?", name, client);

        try {
            boolean applied = session.execute(query).one().getBool("[applied]");

            log.info("Lock released for {} by client {} applied {} timestamp {}", name, client, applied, timestamp);
        } catch (Exception e) {
            log.error("Query to release lock failed to execute for {} by client {}", name, client,  e);
            throw new CannotReleaseLockException("Query failed to execute", e);
        }
    }

    private static void ensureLocksSchemaExists(LockConfig lockConfig, Session session) {
        try {
            Statement query = new SimpleStatement(String.format(
                    "CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {%s}",
                    lockConfig.getReplicationString()
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
        return currentDuration >= timeout.toMillis();
    }
}
