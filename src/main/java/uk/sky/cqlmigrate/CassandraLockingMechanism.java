package uk.sky.cqlmigrate;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

class CassandraLockingMechanism extends LockingMechanism {

    private static final Logger log = LoggerFactory.getLogger(CassandraLockingMechanism.class);

    private final Session session;
    private final ConsistencyLevel consistencyLevel;

    private PreparedStatement insertLockQuery;
    private PreparedStatement deleteLockQuery;
    private boolean isRetryAfterWriteTimeout;

    public CassandraLockingMechanism(Session session, String keyspace, ConsistencyLevel consistencyLevel) {
        super(keyspace + ".schema_migration");
        this.session = session;
        this.consistencyLevel = consistencyLevel;
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
            insertLockQuery = session.prepare("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                    .setConsistencyLevel(consistencyLevel);
            deleteLockQuery = session.prepare("DELETE FROM cqlmigrate.locks WHERE name = ? IF client = ?")
                    .setConsistencyLevel(consistencyLevel);;

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
    public boolean acquire(String clientId) throws CannotAcquireLockException {
        try {
            ResultSet resultSet = session.execute(insertLockQuery.bind(lockName, clientId));
            Row currentLock = resultSet.one();
            // we could already hold the lock and not be aware if a previous acquire had a writetimeout as a timeout is not a failure in cassandra
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
     * If a WriteTimeoutException has previously been thrown this
     * will check if the lock did get successfully deleted.
     *
     * @throws CannotReleaseLockException if any DriverException thrown while executing queries.
     */
    @Override
    public boolean release(String clientId) throws CannotReleaseLockException {
        try {
            ResultSet resultSet = session.execute(deleteLockQuery.bind(lockName, clientId));
            Row result = resultSet.one();

            // if a row doesn't exist then cassandra doesn't send back any columns
            boolean noLockExists = !result.getColumnDefinitions().contains("client");
            if (result.getBool("[applied]") || noLockExists) {
                log.info("Lock released for {} by client {} at: {}", lockName, clientId, System.currentTimeMillis());
                return true;
            }

            String clientReleasingLock = result.getString("client");
            if (!clientReleasingLock.equals(clientId)) {
                if (isRetryAfterWriteTimeout) {
                    log.info("Released lock for client {} in retry attempt after WriteTimeoutException", clientReleasingLock);
                    return true;
                } else {
                    throw new CannotReleaseLockException(
                            String.format("Lock %s attempted to be released by a non lock holder (%s). Current lock holder: %s", lockName, clientId, clientReleasingLock));
                }
            } else {
                log.error("Delete lock query did not get applied but client is still {}. This should never happen.", clientId);
                return false;
            }

        } catch (WriteTimeoutException e) {
            isRetryAfterWriteTimeout = true;
            return false;
        } catch (DriverException e) {
            log.error("Query to release lock failed to execute for {} by client {}", lockName, clientId, e);
            throw new CannotReleaseLockException("Query failed to execute", e);
        }

    }
}
