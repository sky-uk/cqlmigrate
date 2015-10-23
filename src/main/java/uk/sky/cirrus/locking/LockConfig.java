package uk.sky.cirrus.locking;

import java.time.Duration;

public class LockConfig {

    private final Duration pollingInterval, timeout;

    public LockConfig() {
        this.pollingInterval = Duration.ofMillis(500);
        this.timeout = Duration.ofMinutes(1);
    }

    public LockConfig(Duration pollingInterval, Duration timeout) {
        this.pollingInterval = pollingInterval;
        this.timeout = timeout;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public Duration getTimeout() {
        return timeout;
    }
}
