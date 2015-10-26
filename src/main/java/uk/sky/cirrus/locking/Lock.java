package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.util.concurrent.Uninterruptibles;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;
import uk.sky.cirrus.locking.exception.CannotReleaseLockException;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.ConsistencyLevel.ALL;

public class Lock {

    private static final UUID CLIENT = UUID.randomUUID();

    private final String name;
    private final Session session;

    private Lock(String name, Session session) {
        this.name = name;
        this.session = session;
    }

    public static Lock acquire(LockConfig lockConfig, String keyspace, Session session) {

        Duration pollingInterval = lockConfig.getPollingInterval();
        Duration timeout = lockConfig.getTimeout();

        String name = keyspace + ".schema_migration";
        Statement query = new SimpleStatement("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS", name, CLIENT)
                .setConsistencyLevel(ALL);

        long startTime = System.currentTimeMillis();

        while(true) {

            ResultSet resultSet;
            try {
                resultSet = session.execute(query);
            } catch (DriverException e) {
                throw new CannotAcquireLockException("Query failed to execute", e);
            }

            Row lock = resultSet.one();
            boolean lockAcquired = lock.getBool("[applied]");

            if (lockAcquired) {
                return new Lock(name, session);
            } else {
                Uninterruptibles.sleepUninterruptibly(pollingInterval.toMillis(), TimeUnit.MILLISECONDS);

                if (timedOut(timeout, startTime)) {
                    throw new CannotAcquireLockException("Lock currently in use by client: " + lock.getUUID("client"));
                }
            }
        }

    }

    private static boolean timedOut(Duration timeout, long startTime) {
        long currentDuration = System.currentTimeMillis() - startTime;
        return currentDuration >= timeout.toMillis();
    }

    public void release() {
        Statement query = new SimpleStatement("DELETE FROM locks.locks WHERE name = ?", name)
                .setConsistencyLevel(ALL);

        try {
            session.execute(query);
        } catch (DriverException e) {
            throw new CannotReleaseLockException("Query failed to execute", e);
        }
    }
}
