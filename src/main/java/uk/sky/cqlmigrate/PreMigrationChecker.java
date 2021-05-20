package uk.sky.cqlmigrate;

import static uk.sky.cqlmigrate.SchemaUpdates.SCHEMA_UPDATES_TABLE;

public class PreMigrationChecker {

    private final SessionContext sessionContext;
    private final String keyspace;

    public PreMigrationChecker(SessionContext sessionContext, String keyspace) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
    }

    boolean migrationIsNeeded() {
        return !keyspaceExists() || !schemaUpdatesTableExists();
    }

    private boolean keyspaceExists() {
        return sessionContext.getSession().getCluster().getMetadata().getKeyspace(keyspace) != null;
    }

    private boolean schemaUpdatesTableExists() {
        return sessionContext.getSession().getCluster().getMetadata().getKeyspace(keyspace).getTable(SCHEMA_UPDATES_TABLE) != null;
    }


}
