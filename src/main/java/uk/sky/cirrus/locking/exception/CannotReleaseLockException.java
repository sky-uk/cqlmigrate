package uk.sky.cirrus.locking.exception;

public class CannotReleaseLockException extends LockException {

    public CannotReleaseLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
