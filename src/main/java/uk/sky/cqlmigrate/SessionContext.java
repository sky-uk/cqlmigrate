package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

class SessionContext {

    private final Session session;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    private final ClusterHealth clusterHealth;
    private boolean clusterHealthChecked = false;

    SessionContext(Session session, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel, ClusterHealth clusterHealth) {
        this.session = session;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.clusterHealth = clusterHealth;
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

    public void checkClusterHealth() {
        if (!clusterHealthChecked) {
            clusterHealth.check();
            clusterHealthChecked = true;
        }
    }
}
