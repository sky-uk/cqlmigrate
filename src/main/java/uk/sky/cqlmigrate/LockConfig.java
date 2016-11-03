package uk.sky.cqlmigrate;

import java.time.Duration;
import java.util.UUID;

public class LockConfig {

    protected final Duration pollingInterval, timeout;
    protected final String clientId;

    protected LockConfig(Duration pollingInterval, Duration timeout, String clientId) {
        this.pollingInterval = pollingInterval;
        this.timeout = timeout;
        this.clientId = clientId;
    }

    Duration getPollingInterval() {
        return pollingInterval;
    }

    Duration getTimeout() {
        return timeout;
    }

    String getClientId() {
        return clientId;
    }

    public static LockConfigBuilder builder() {
        return new LockConfigBuilder();
    }

    public static class LockConfigBuilder {

        protected Duration pollingInterval = Duration.ofMillis(500);
        protected Duration timeout = Duration.ofMinutes(1);
        protected String clientId = UUID.randomUUID().toString();

        protected LockConfigBuilder() {}

        /**
         * Duration to wait after each attempt to acquire the lock.
         *
         * @param pollingInterval defaults to 500 milliseconds.
         * @return this
         * @throws IllegalArgumentException if value is less than 0
         */
        public LockConfigBuilder withPollingInterval(Duration pollingInterval) {
            if (pollingInterval.toMillis() < 0)
                throw new IllegalArgumentException("Polling interval must be positive: " + pollingInterval.toMillis());

            this.pollingInterval = pollingInterval;
            return this;
        }

        /**
         * Duration to attempt to acquire lock for.
         *
         * @param timeout defaults to 1 minute
         * @return this
         * @throws IllegalArgumentException if value is less than 0
         */
        public LockConfigBuilder withTimeout(Duration timeout) {
            if (timeout.toMillis() < 0)
                throw new IllegalArgumentException("Timeout must be positive: " + timeout.toMillis());

            this.timeout = timeout;
            return this;
        }

        public LockConfig build() {
            return new LockConfig(pollingInterval, timeout, clientId);
        }
    }
}
