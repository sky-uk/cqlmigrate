package uk.sky.cqlmigrate.exception;

/**
 * Thrown if any of the queries to release lock fail.
 */
public class CannotReleaseLockException extends LockException {

    public CannotReleaseLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public CannotReleaseLockException(String message) {
        super(message);
    }
}
