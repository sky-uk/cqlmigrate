package uk.sky.cqlmigrate;

import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

abstract class LockingMechanism {

    protected final String lockName;

    public LockingMechanism(String lockName) {
        this.lockName = lockName;
    }

    public void init() {

    }

    /**
     * Returns true if successfully acquired lock.
     *
     * @param clientId client to acquire the lock for
     * @return if the lock was successfully acquired. A false value means that the lock was not acquired,
     * or that the result of the operation is unknown.
     * @throws CannotAcquireLockException if any fatal failure occurs when trying to acquire lock.
     */
    abstract public boolean acquire(String clientId) throws CannotAcquireLockException;

    /**
     * @param clientId client to release the lock for
     * @return true if the lock was successfully released. A false value means that the lock was not released,
     * or that the result of the operation is unknown.
     * @throws CannotReleaseLockException if any fatal failure occurs when trying to release lock.
     */
    abstract public boolean release(String clientId) throws CannotReleaseLockException;

    public String getLockName() {
        return lockName;
    }
}
