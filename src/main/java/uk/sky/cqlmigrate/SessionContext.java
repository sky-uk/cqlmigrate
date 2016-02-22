package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

class SessionContext {

    private final Session session;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    SessionContext(Session session, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
        this.session = session;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
    }

    public Session getSession() {
        return session;
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }
}
