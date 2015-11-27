package uk.sky.cqlmigrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

import java.time.Duration;

/**
 * Each instance attempts to acquire the lock.
 */
class Lock {

    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private final LockingMechanism lockingMechanism;
    private final String clientId;

    private Lock(LockingMechanism lockingMechanism, String clientId) {
        this.lockingMechanism = lockingMechanism;
        this.clientId = clientId;
    }

    /**
     * If the lock is successfully acquired, the lock is returned, otherwise
     * this will wait until the lock has been released. If a lock cannot
     * be acquired within the configured timeout interval, an exception is thrown.
     *
     * @param lockingMechanism {@code LockingMechanism} to use to acquire the lock
     * @param lockConfig       {@code LockConfig} for configuring the lock polling interval, timeout and client id
     * @return the {@code Lock} object
     * @throws CannotAcquireLockException if this cannot acquire lock within the specified time interval or locking mechanism fails
     */
    public static Lock acquire(LockingMechanism lockingMechanism, LockConfig lockConfig) throws CannotAcquireLockException {

        lockingMechanism.init();

        String lockName = lockingMechanism.getLockName();
        String clientId = lockConfig.getClientId();

        int acquireAttempts = 1;

        log.info("Attempting to acquire lock for '{}', using client id '{}'", lockName, clientId);
        long startTime = System.currentTimeMillis();

        while (true) {
            if (lockingMechanism.acquire(clientId)) {
                log.info("Lock acquired for '{}' by client '{}' after {} attempts", lockName, clientId, acquireAttempts);
                return new Lock(lockingMechanism, clientId);
            } else {
                waitToAcquire(lockConfig, lockName, clientId, acquireAttempts, startTime);
            }

            acquireAttempts++;
        }
    }

    /**
     * Will release the lock using the locking mechanism.
     *
     * @throws CannotReleaseLockException locking mechanism fails to release lock
     */
    public void release() throws CannotReleaseLockException {
        lockingMechanism.release(clientId);
    }

    private static void waitToAcquire(LockConfig lockConfig, String lockName, String clientId, int acquireAttempts, long startTime) {

        if (timedOut(lockConfig.getTimeout(), startTime)) {
            log.warn("Unable to acquire lock for {} after {} attempts, time tried: {}", clientId, acquireAttempts, (System.currentTimeMillis() - startTime));
            throw new CannotAcquireLockException("Lock currently in use");
        }

        try {
            Thread.sleep(lockConfig.getPollingInterval().toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CannotAcquireLockException(String.format("Polling to acquire lock %s for client %s was interrupted", lockName, clientId), e);

        }
    }

    private static boolean timedOut(Duration timeout, long startTime) {
        long currentDuration = System.currentTimeMillis() - startTime;
        return currentDuration >= timeout.toMillis();
    }
}
