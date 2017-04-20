package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;

import java.time.Duration;

public class CassandraLockConfig extends LockConfig {

    private CassandraLockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure) {
        super(pollingInterval, timeout, clientId, unlockOnFailure);
    }

    @Override
    public LockingMechanism getLockingMechanism(Session session, String keySpace) {
        return new CassandraLockingMechanism(session, keySpace);
    }
}
