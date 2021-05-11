package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;

import static java.lang.String.format;

class CassandraLockingMechanism extends LockingMechanism {

    private static final Logger log = LoggerFactory.getLogger(CassandraLockingMechanism.class);

    private final CqlSession session;
    private final ConsistencyLevel consistencyLevel;
    private final String lockKeyspace;

    private PreparedStatement selectLockQuery;
    private PreparedStatement insertLockQuery;
    private PreparedStatement deleteLockQuery;
    private boolean isRetryAfterWriteTimeout;

    public CassandraLockingMechanism(CqlSession session, String keyspace, ConsistencyLevel consistencyLevel, String lockKeyspace) {
        super(keyspace + ".schema_migration");
        this.session = session;
        this.consistencyLevel = consistencyLevel;
        this.lockKeyspace = lockKeyspace;
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
            String selectQuery = format("SELECT name,client FROM %s.locks LIMIT 1", lockKeyspace);
            String insertQuery = format("INSERT INTO %s.locks (name, client) VALUES (?, ?) IF NOT EXISTS", lockKeyspace);
            String deleteQuery = format("DELETE FROM %s.locks WHERE name = ? IF client = ?", lockKeyspace);

            selectLockQuery = session.prepare(SimpleStatement.newInstance(selectQuery).setConsistencyLevel(consistencyLevel));
            insertLockQuery = session.prepare(SimpleStatement.newInstance(insertQuery).setConsistencyLevel(consistencyLevel));
            deleteLockQuery = session.prepare(SimpleStatement.newInstance(deleteQuery).setConsistencyLevel(consistencyLevel));
        } catch (DriverException e) {
            throw new CannotAcquireLockException("Query to prepare locks queries failed", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns true if successfully inserted lock.
     * Returns true if current lock is owned by this client.
     * Returns false if WriteTimeoutException thrown.
     *
     * @throws CannotAcquireLockException if any DriverException thrown while executing queries.
     */
    @Override
    public boolean acquire(String clientId) throws CannotAcquireLockException {
        try {
            verifyClusterIsHealthy();
            ResultSet resultSet = session.execute(insertLockQuery.bind(lockName, clientId));
            Row currentLock = resultSet.one();
            // we could already hold the lock and not be aware if a previous acquire had a writetimeout as a timeout is not a failure in cassandra
            //TODO this needs to be tested
            if (currentLock != null && (currentLock.getBoolean("[applied]") || clientId.equals(currentLock.getString("client")))) {
                return true;
            } else {
                log.info("Lock currently held by {}", currentLock);
                return false;
            }
        } catch (WriteTimeoutException wte) {
            log.warn("Query to acquire lock for {} failed to execute: {}", clientId, wte.getMessage());
            return false;
        } catch (DriverException de) {
            throw new CannotAcquireLockException(format("Query to acquire lock %s for client %s failed to execute", lockName, clientId), de);
        }
    }

    /**
     * Verify that a select of the locks completes successfully
     *
     * @throws DriverException propagated from the session.execute() call.
     */
    private void verifyClusterIsHealthy() {
        session.execute(selectLockQuery.bind());
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
            if (result.getBoolean("[applied]") || noLockExists) {
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
