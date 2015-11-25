package uk.sky.cirrus.locking;

import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

public abstract class LockingMechanism {

    protected final String lockName;
    protected final String clientId;

    public LockingMechanism(String lockName, String clientId) {
        this.lockName = lockName;
        this.clientId = clientId;
    }

    public void init() {

    }

    abstract public boolean acquire() throws CannotAcquireLockException;
    abstract public boolean release() throws CannotReleaseLockException;

    public String getLockName() {
        return lockName;
    }

    public String getClientId() {
        return clientId;
    }
}
