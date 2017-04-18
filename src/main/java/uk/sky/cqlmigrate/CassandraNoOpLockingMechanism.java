package uk.sky.cqlmigrate;

import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

class CassandraNoOpLockingMechanism extends LockingMechanism {
    public CassandraNoOpLockingMechanism() {
        super("NoOp");
    }

    /**
     * Nothing to prepare.
     */
    @Override
    public void init() throws CannotAcquireLockException {

    }

    /**
     * {@inheritDoc}
     * <p>
     * Always a success, no lock to acquire.
     */
    @Override
    public boolean acquire(String clientId) throws CannotAcquireLockException {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Always a success, no lock to release.
     */
    @Override
    public boolean release(String clientId) throws CannotReleaseLockException {
        return true;
    }
}
