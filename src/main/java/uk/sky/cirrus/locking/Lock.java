package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;

import java.util.UUID;

import static com.datastax.driver.core.ConsistencyLevel.ALL;

public class Lock {

    private static final UUID CLIENT = UUID.randomUUID();
    private static final long TIMEOUT = 3000;

    private final String name;
    private final Session session;

    private Lock(String name, Session session) {
        this.name = name;
        this.session = session;
    }

    public static Lock acquire(String keyspace, Session session) {
        String name = keyspace + ".schema_migration";
        Statement query = new SimpleStatement("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS", name, CLIENT)
                .setConsistencyLevel(ALL);

        long startTime = System.currentTimeMillis();

        while(true) {
            ResultSet resultSet = session.execute(query);
            Row lock = resultSet.one();
            boolean lockAcquired = lock.getBool("[applied]");

            if (lockAcquired) {
                return new Lock(name, session);
            } else {
                long currentDuration = System.currentTimeMillis() - startTime;
                if (currentDuration >= TIMEOUT) {
                    throw new CannotAcquireLockException("Lock currently in use by client: " + lock.getUUID("client"));
                }
            }
        }

    }

    public void release() {
        Statement query = new SimpleStatement("DELETE FROM locks.locks WHERE name = ?", name)
                .setConsistencyLevel(ALL);
        session.execute(query);
    }
}
