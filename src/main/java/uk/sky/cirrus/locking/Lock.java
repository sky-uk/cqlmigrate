package uk.sky.cirrus.locking;

import com.datastax.driver.core.Session;
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
 */
public class Lock {

    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private static LockingMechanism lockingMechanism;

    private final UUID client;
    private boolean released;

    private Lock(String name, Session session, UUID client) {
        this.client = client;
    }

    /**
     * @param clientId   Identifier for the owner of the lock
     * @param lockConfig {@code LockConfig} for configuring the lock to migrate the schema
     * @param keyspace   Name of the keyspace which is to be used for migration
     * @param session    Cassandra {@code Session} to use while acquiring the lock
     * @return the {@code Lock} object
     * @throws CannotAcquireLockException if instance cannot acquire lock within the specified time interval or execution of query to insert lock fails
     */
    static Lock acquire(LockConfig lockConfig, String keyspace, Session session, UUID clientId) {

        lockingMechanism = new CassandraLockingMechanism(session, keyspace, clientId);
        String lockName = lockingMechanism.getLockName();

        int acquireAttempts = 1;

        log.info("Attempting to acquire lock for '{}', using client id '{}'", lockName, clientId);
        long startTime = System.currentTimeMillis();

        while (true) {
            if (lockingMechanism.acquire()) {
                log.info("Lock acquired for '{}' by client '{}' after {} attempts", lockName, clientId, acquireAttempts);
                return new Lock(lockName, session, clientId);
            } else {
                waitToAcquire(lockConfig, clientId, acquireAttempts, startTime);
            }

            acquireAttempts++;
        }
    }

    /**
     * @throws CannotReleaseLockException if execution of query to remove lock fails
     */
    void release() {
       while(true) {
           if (released || lockingMechanism.release()) {
               released = true;
               return;
           }
       }
    }


    private static void waitToAcquire(LockConfig lockConfig, UUID clientId, int acquireAttempts, long startTime) {

        if (timedOut(lockConfig.getTimeout(), startTime)) {
            log.warn("Unable to acquire lock for {} after {} attempts, time tried: {}", clientId, acquireAttempts, (System.currentTimeMillis() - startTime));
            throw new CannotAcquireLockException("Lock currently in use");
        }

        try {
            Thread.sleep(lockConfig.getPollingInterval().toMillis());
        } catch (InterruptedException e) {
            log.debug("Lock {} was interrupted", clientId);
            Thread.currentThread().interrupt();
        }
    }

    private static boolean timedOut(Duration timeout, long startTime) {
        long currentDuration = System.currentTimeMillis() - startTime;
        return currentDuration >= timeout.toMillis();
    }

    public UUID getClient() {
        return this.client;
    }

    /**
     * Clients can make use of this getter to check if they have an active or a stale lock
     *
     * @return if the current lock is released
     */
    public boolean isReleased() {
        return this.released;
    }
}
