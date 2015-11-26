package uk.sky.cqlmigrate;

import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

abstract class LockingMechanism {

    protected final String lockName;
    protected final String clientId;

    public LockingMechanism(String lockName, String clientId) {
        this.lockName = lockName;
        this.clientId = clientId;
    }

    public void init() {

    }

    /**
     * Returns true if successfully acquired lock.
     *
     * @return result
     * @throws CannotAcquireLockException if any fatal failure occurs when trying to acquire lock.
     */
    abstract public boolean acquire() throws CannotAcquireLockException;

    /**
     * @throws CannotReleaseLockException if any failure occurs when trying to release lock.
     */
    abstract public void release() throws CannotReleaseLockException;

    public String getLockName() {
        return lockName;
    }

    public String getClientId() {
        return clientId;
    }
}
