package uk.sky.cirrus.locking;

import com.datastax.driver.core.*;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;

import java.util.UUID;

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

        ResultSet resultSet = session.execute("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS", name, CLIENT);
        Row lock = resultSet.one();
        boolean lockAcquired = lock.getBool("[applied]");

        if (lockAcquired) {
            return new Lock(name, session);
        } else {
            throw new CannotAcquireLockException("Lock currently in use by client: " + lock.getUUID("client"));
        }
    }

    public void release() {
        session.execute("DELETE FROM locks.locks WHERE name = ? IF EXISTS", name);
    }
}
