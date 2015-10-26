package uk.sky.cirrus.locking.exception;

public class CannotAcquireLockException extends LockException {

    public CannotAcquireLockException(String message) {
       super(message);
    }

    public CannotAcquireLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
