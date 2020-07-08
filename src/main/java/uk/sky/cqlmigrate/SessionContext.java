package uk.sky.cqlmigrate;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

class SessionContext {

    private final CqlSession session;
    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;
    private final ClusterHealth clusterHealth;
    private boolean clusterHealthChecked = false;

    SessionContext(CqlSession session, ConsistencyLevel readConsistencyLevel, ConsistencyLevel writeConsistencyLevel, ClusterHealth clusterHealth) {
        this.session = session;
        this.readConsistencyLevel = readConsistencyLevel;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.clusterHealth = clusterHealth;
    }

    public CqlSession getSession() {
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
