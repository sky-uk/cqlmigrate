package uk.sky.cirrus.exception;

public class ClusterUnhealthyException extends RuntimeException {
    public ClusterUnhealthyException(String message) {
        super(message);
    }
}
