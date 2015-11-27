package uk.sky.cqlmigrate;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

class CassandraLockingMechanism extends LockingMechanism {

    private static final Logger log = LoggerFactory.getLogger(CassandraLockingMechanism.class);

    private final Session session;

    private PreparedStatement insertLockQuery;
    private PreparedStatement deleteLockQuery;

    public CassandraLockingMechanism(Session session, String keyspace, CassandraLockConfig lockConfig) {
        super(keyspace + ".schema_migration", lockConfig.getClientId());
        this.session = session;
    }

    /**
     * Prepares queries for acquiring and releasing lock.
     *
     * @throws CannotAcquireLockException if any DriverException thrown while executing queries.
     */
    @Override
    public void init() throws CannotAcquireLockException {
        super.init();

        try {
            insertLockQuery = session.prepare("INSERT INTO cqlmigrate_locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS");
            deleteLockQuery = session.prepare("DELETE FROM cqlmigrate_locks.locks WHERE name = ? IF client = ?");

        } catch (DriverException e) {
            throw new CannotAcquireLockException("Query to prepare locks queries failed", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if successfully inserted lock.
     * Returns false if current lock is owned by this client.
     * Returns false if WriteTimeoutException thrown.
     *
     * @throws CannotAcquireLockException if any DriverException thrown while executing queries.
     */
    @Override
    public boolean acquire() throws CannotAcquireLockException {
        try {
            ResultSet resultSet = session.execute(insertLockQuery.bind(lockName, clientId));
            Row currentLock = resultSet.one();
            if (currentLock.getBool("[applied]") || clientId.equals(currentLock.getString("client"))) {
                return true;
            } else {
                log.info("Lock currently held by {}", currentLock);
                return false;
            }
        } catch (WriteTimeoutException wte) {
            log.warn("Query to acquire lock for {} failed to execute: {}", clientId, wte.getMessage());
            return false;
        } catch (DriverException de) {
            throw new CannotAcquireLockException(String.format("Query to acquire lock %s for client %s failed to execute", lockName, clientId), de);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Deletes lock from locks table.
     * Will retry to release lock if WriteTimeoutException thrown.
     * If a WriteTimeoutException has previously been thrown this
     * will check if the lock did actually successfully deleted.
     *
     * @throws CannotAcquireLockException if any DriverException thrown while executing queries.
     */
    @Override
    public void release() throws CannotReleaseLockException {
        boolean isRetryAfterWriteTimeout = false;
        while (true) {
            try {
                ResultSet resultSet = session.execute(deleteLockQuery.bind(lockName, clientId));
                Row result = resultSet.one();

                if (result.getBool("[applied]") || !result.getColumnDefinitions().contains("client")) {
                    log.info("Lock released for {} by client {} at: {}", lockName, clientId, System.currentTimeMillis());
                    return;
                }

                String clientReleasingLock = result.getString("client");
                if (!clientReleasingLock.equals(clientId)) {
                    if (isRetryAfterWriteTimeout) {
                        log.info("Released lock for client {} in retry attempt after WriteTimeoutException", clientReleasingLock);
                        return;
                    } else {
                        throw new CannotReleaseLockException(
                                String.format("Lock %s attempted to be released by a non lock holder (%s). Current lock holder: %s", lockName, clientId, clientReleasingLock));
                    }
                }

            } catch (WriteTimeoutException e) {
                isRetryAfterWriteTimeout = true;
                waitToRetryRelease();
            } catch (DriverException e) {
                log.error("Query to release lock failed to execute for {} by client {}", lockName, clientId, e);
                throw new CannotReleaseLockException("Query failed to execute", e);
            }
        }
    }

    private void waitToRetryRelease() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.warn("Thread sleep interrupted with {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
