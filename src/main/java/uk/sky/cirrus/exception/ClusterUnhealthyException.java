package uk.sky.cirrus.exception;

/**
 * Thrown if any nodes are down or the schema is not
 * in agreement before running migration.
 */
public class ClusterUnhealthyException extends RuntimeException {
    public ClusterUnhealthyException(String message) {
        super(message);
    }
}
