package uk.sky.cirrus.locking;

import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

public interface LockingMechanism {

    boolean acquire() throws CannotAcquireLockException;
    boolean release() throws CannotReleaseLockException;
    String getLockName();
}
