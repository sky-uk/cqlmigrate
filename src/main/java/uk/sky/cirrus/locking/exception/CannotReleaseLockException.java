package uk.sky.cirrus.locking.exception;

/**
 * Thrown if any of the queries to release lock fail.
 */
public class CannotReleaseLockException extends LockException {

    public CannotReleaseLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
