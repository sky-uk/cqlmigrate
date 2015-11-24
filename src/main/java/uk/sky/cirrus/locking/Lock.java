package uk.sky.cirrus.locking;

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

    private final LockingMechanism lockingMechanism;
    private final UUID clientId;

    private Lock(LockingMechanism lockingMechanism) {
        this.lockingMechanism = lockingMechanism;
        this.clientId = lockingMechanism.getClientId();
    }

    /**
     * @param lockConfig {@code LockConfig} for configuring the lock to migrate the schema
     * @return the {@code Lock} object
     * @throws CannotAcquireLockException if instance cannot acquire lock within the specified time interval or execution of query to insert lock fails
     */
    static Lock acquire(LockingMechanism lockingMechanism, LockConfig lockConfig) {

        String lockName = lockingMechanism.getLockName();
        UUID clientId = lockingMechanism.getClientId();

        int acquireAttempts = 1;

        log.info("Attempting to acquire lock for '{}', using client id '{}'", lockName, clientId);
        long startTime = System.currentTimeMillis();

        while (true) {
            if (lockingMechanism.acquire()) {
                log.info("Lock acquired for '{}' by client '{}' after {} attempts", lockName, clientId, acquireAttempts);
                return new Lock(lockingMechanism);
            } else {
                waitToAcquire(lockConfig, clientId, acquireAttempts, startTime);
            }

            acquireAttempts++;
        }
    }

    /**
     * @throws CannotReleaseLockException if execution of query to remove lock fails
     */
    public void release() {
       lockingMechanism.release();
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
        return this.clientId;
    }
}
