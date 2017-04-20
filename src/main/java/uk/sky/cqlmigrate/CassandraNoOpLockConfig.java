package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;

import java.time.Duration;

public class CassandraNoOpLockConfig extends LockConfig {

    private CassandraNoOpLockConfig(Duration pollingInterval, Duration timeout, String clientId, boolean unlockOnFailure) {
        super(pollingInterval, timeout, clientId, unlockOnFailure);
    }

    @Override
    public LockingMechanism getLockingMechanism(Session session, String keySpace) {
        return  new CassandraNoOpLockingMechanism();
    }
}
