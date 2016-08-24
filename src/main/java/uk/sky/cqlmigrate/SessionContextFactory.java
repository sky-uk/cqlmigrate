package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;

class SessionContextFactory {
    SessionContext getInstance(Session session, CqlMigratorConfig cqlMigratorConfig) {
        ClusterHealth clusterHealth = new ClusterHealth(session.getCluster());
        return new SessionContext(session, cqlMigratorConfig.getReadConsistencyLevel(), cqlMigratorConfig.getWriteConsistencyLevel(), clusterHealth);
    }
}
