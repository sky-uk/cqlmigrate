package uk.sky.cqlmigrate;

public class PreMigrationChecker {

    private final SessionContext sessionContext;
    private final String keyspace;

    public PreMigrationChecker(SessionContext sessionContext, String keyspace) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
    }

    boolean migrationIsNeeded() {
        return !keyspaceExists();
    }

    private boolean keyspaceExists() {
        return sessionContext.getSession().getCluster().getMetadata().getKeyspace(keyspace) != null;
    }

}
