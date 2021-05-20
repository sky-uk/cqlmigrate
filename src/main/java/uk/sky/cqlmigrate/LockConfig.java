package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;

import java.time.Duration;
import java.util.UUID;

public class LockConfig {

    protected final Duration pollingInterval, timeout;
    protected final String clientId;
    protected final boolean unlockOnFailure;

    protected LockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure) {
        this.pollingInterval = pollingInterval;
        this.timeout = timeout;
        this.clientId = clientId;
        this.unlockOnFailure = unlockOnFailure;
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

    boolean unlockOnFailure() {
        return unlockOnFailure;
    }

    public LockingMechanism getLockingMechanism(CqlSession session, String keySpace) {
        throw new UnsupportedOperationException();
    }

    public static LockConfigBuilder builder() {
        return new LockConfigBuilder();
    }

    public static class LockConfigBuilder {

        protected Duration pollingInterval = Duration.ofMillis(500);
        protected Duration timeout = Duration.ofMinutes(1);
        protected String clientId = UUID.randomUUID().toString();
        protected boolean unlockOnFailure;

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

        /**
         * Release the lock in case of failure.
         * <p>
         * Note: default behavior is to leave the lock behind if any failures occurred during migration.
         * This was done to prevent accidental data corruption and bring manual attention to the problem.
         * <p>
         * Use 'unlockOnFailure' to override the default behavior and force cqlmigrate to release the lock.
         *
         * @return this
         */
        public LockConfigBuilder unlockOnFailure() {
            this.unlockOnFailure = true;
            return this;
        }

        public LockConfig build() {
            return new LockConfig(pollingInterval, timeout, clientId, unlockOnFailure);
        }
    }
}
