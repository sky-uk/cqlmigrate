package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;

class SessionContextFactory {
    SessionContext getInstance(CqlSession session, CqlMigratorConfig cqlMigratorConfig) {
        ClusterHealth clusterHealth = new ClusterHealth(session);
        return new SessionContext(session, cqlMigratorConfig.getReadConsistencyLevel(), cqlMigratorConfig.getWriteConsistencyLevel(), clusterHealth);
    }
}
