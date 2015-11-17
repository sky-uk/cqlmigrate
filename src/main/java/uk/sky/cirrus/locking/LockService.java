package uk.sky.cirrus.locking;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockService {

    private static final Logger log = LoggerFactory.getLogger(LockService.class);
    private static final Statement SCHEMA_VERIFICATION_QUERY = new SimpleStatement("SELECT keyspace_name FROM system.schema_keyspaces WHERE keyspace_name = 'locks'");

    static final AtomicBoolean SCHEMA_CREATED = new AtomicBoolean(false);

    /**
     * Service class which co-ordinates providing the {@code Lock} object to multiple threads trying to acquire the lock.
     * Ensures necessary metadata / schema needed by {@code Lock} is created if not already available. Creation of the schema needed by {@code Lock} is synchronized for
     *
     * @param lockConfig {@code LockConfig} for configuring the lock to migrate the schema
     * @param keyspace   Name of the keyspace which is to be used for migration
     * @param session    Cassandra {@code Session} to use while acquiring the lock
     * @return the {@code Lock} object
     * @throws CannotAcquireLockException if instance cannot acquire lock within the specified time interval or execution of query to insert lock fails
     */
    public static Lock acquire(LockConfig lockConfig, String keyspace, Session session) {
        while(!SCHEMA_CREATED.get()){
            ensureLocksSchemaExists(lockConfig, session);
        }
        return Lock.acquire(lockConfig, keyspace, session, UUID.randomUUID());
    }

    /**
     * @param lock {@code Lock} to be released
     * @throws CannotReleaseLockException if execution of query to remove lock fails
     */

    public static void release(final Lock lock) {
        lock.release();
    }

    private static void ensureLocksSchemaExists(LockConfig lockConfig, Session session) {
        final Row locksKeyspace = session.execute(SCHEMA_VERIFICATION_QUERY).one();

        if(locksKeyspace == null) {
            createLocksSchema(lockConfig, session);
        } else {
            SCHEMA_CREATED.compareAndSet(false, true);
        }
    }

    private static synchronized void createLocksSchema(final LockConfig lockConfig, final Session session) {

        if(SCHEMA_CREATED.get()){
            return;
        }

        try {
            final Statement createKeyspaceQuery = new SimpleStatement(
                    String.format("CREATE KEYSPACE IF NOT EXISTS locks WITH replication = {%s}", lockConfig.getReplicationString())
            );
            session.execute(createKeyspaceQuery);

            final Statement createTableQuery = new SimpleStatement("CREATE TABLE IF NOT EXISTS locks.locks (name text PRIMARY KEY, client uuid)");
            session.execute(createTableQuery);
            SCHEMA_CREATED.compareAndSet(false, true);
        } catch (DriverException e) {
            log.warn("Query to create locks keyspace or locks table failed to execute", e);
            throw new CannotAcquireLockException("Query to create locks schema failed to execute", e);
        }

    }
}
