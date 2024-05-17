package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

class TableCheckerFactory {

    public static final String AWSK_CLUSTER_NAME = "Amazon Keyspaces";

    TableChecker getInstance(CqlSession session, CqlMigratorConfig cqlMigratorConfig) {
        return isAWSK(session) ? new AwskTableChecker(cqlMigratorConfig.getTableCheckerTimeout()) : new NoOpTableChecker();
    }

    private boolean isAWSK(CqlSession session) {
        ResultSet result = session.execute("SELECT cluster_name FROM system.local WHERE key='local';");
        return AWSK_CLUSTER_NAME.equals(result.one().getString("cluster_name"));
    }
}
