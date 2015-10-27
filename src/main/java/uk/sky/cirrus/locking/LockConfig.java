package uk.sky.cirrus.locking;

import org.joda.time.Duration;

public class LockConfig {

    private final Duration pollingInterval, timeout;
    private final String replicationClass;
    private final int replicationFactor;

    public LockConfig() {
        this.pollingInterval = Duration.millis(500);
        this.timeout = Duration.standardMinutes(1);
        this.replicationClass = "SimpleStrategy";
        this.replicationFactor = 1;
    }

    public LockConfig(Duration pollingInterval, Duration timeout, String replicationClass, int replicationFactor) {
        this.pollingInterval = pollingInterval;
        this.timeout = timeout;
        this.replicationClass = replicationClass;
        this.replicationFactor = replicationFactor;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public String getReplicationClass() {
        return replicationClass;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }
}
