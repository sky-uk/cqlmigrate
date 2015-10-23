package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;

import java.util.UUID;

import static com.datastax.driver.core.ConsistencyLevel.*;

public class Lock {

    private static final UUID CLIENT = UUID.randomUUID();

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

        ResultSet resultSet = session.execute(query);
        Row lock = resultSet.one();
        boolean lockAcquired = lock.getBool("[applied]");

        if (lockAcquired) {
            return new Lock(name, session);
        } else {
            throw new CannotAcquireLockException("Lock currently in use by client: " + lock.getUUID("client"));
        }
    }

    public void release() {
        Statement query = new SimpleStatement("DELETE FROM locks.locks WHERE name = ?", name)
                .setConsistencyLevel(ALL);
        session.execute(query);
    }
}
