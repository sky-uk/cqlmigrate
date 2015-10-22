package uk.sky.cirrus.locking.exception;

public class CannotAcquireLockException extends RuntimeException {

    public CannotAcquireLockException(String message) {
       super(message);
    }
}
