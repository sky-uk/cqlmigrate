package uk.sky.cqlmigrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

import java.util.concurrent.TimeoutException;

/**
 * Each instance attempts to acquire the lock.
 */
class Lock {

    private static final Logger log = LoggerFactory.getLogger(Lock.class);

    private final LockingMechanism lockingMechanism;
    private final LockConfig lockConfig;

    private Lock(LockingMechanism lockingMechanism, LockConfig clientId) {
        this.lockingMechanism = lockingMechanism;
        this.lockConfig = clientId;
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
        try {
            log.info("Attempting to acquire lock for '{}', using client id '{}'", lockName, lockConfig.getClientId());
            return RetryTask.attempt(() -> lockingMechanism.acquire(clientId))
                    .withTimeout(lockConfig.getTimeout())
                    .withPollingInterval(lockConfig.getPollingInterval())
                    .untilSuccess()
                    .thenReturn(() -> new Lock(lockingMechanism, lockConfig));
        } catch (TimeoutException te) {
            log.warn("Unable to acquire lock for {}", lockConfig.getClientId(), te);
            throw new CannotAcquireLockException("Lock currently in use", te);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CannotAcquireLockException(String.format("Polling to acquire lock %s for client %s was interrupted", lockName, lockConfig.getClientId()), e);
        }
    }

    /**
     * Will release the lock using the locking mechanism. If a lock cannot
     * be released within the configured timeout interval, an exception is thrown.
     *
     * @throws CannotReleaseLockException if this cannot release lock within the specified time interval or locking mechanism fails
     */
    public void release() throws CannotReleaseLockException {
        String lockName = lockingMechanism.getLockName();
        try {
            log.info("Attempting to release lock for '{}', using client id '{}'", lockName, lockConfig.getClientId());
            RetryTask.attempt(() -> lockingMechanism.release(lockConfig.getClientId()))
                    .withTimeout(lockConfig.getTimeout())
                    .withPollingInterval(lockConfig.getPollingInterval())
                    .untilSuccess();
        } catch (TimeoutException te) {
            log.warn("Unable to release lock for {}", lockConfig.getClientId(), te);
            throw new CannotReleaseLockException("Failed to release lock", te);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CannotReleaseLockException(String.format("Polling to release lock %s for client %s was interrupted", lockName, lockConfig.getClientId()), e);
        }
    }


}
