package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

class ExecutionInfo {

    private final Session session;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;


    ExecutionInfo(Session session, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel) {
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
