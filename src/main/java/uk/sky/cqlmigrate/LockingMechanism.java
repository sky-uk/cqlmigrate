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
     * @return result
     * @throws CannotAcquireLockException if any fatal failure occurs when trying to acquire lock.
     */
    abstract public boolean acquire(String clientId) throws CannotAcquireLockException;

    /**
     * @param clientId client to release the lock for
     * @throws CannotReleaseLockException if any failure occurs when trying to release lock.
     */
    abstract public void release(String clientId) throws CannotReleaseLockException;

    public String getLockName() {
        return lockName;
    }
}
