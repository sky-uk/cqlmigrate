package uk.sky.cqlmigrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static uk.sky.cqlmigrate.SchemaUpdates.SCHEMA_UPDATES_TABLE;

public class PreMigrationChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreMigrationChecker.class);

    private final SessionContext sessionContext;
    private final String keyspace;
    private final SchemaChecker schemaChecker;
    private final CqlPaths paths;

    public PreMigrationChecker(SessionContext sessionContext, String keyspace, SchemaChecker schemaChecker, CqlPaths paths) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
        this.schemaChecker = schemaChecker;
        this.paths = paths;
    }

    boolean migrationIsNeeded() {
        return !keyspaceExists() || !schemaUpdatesTableExists() || !allMigrationFilesApplied();
    }

    private boolean keyspaceExists() {
        return sessionContext.getSession().getCluster().getMetadata().getKeyspace(keyspace) != null;
    }

    private boolean schemaUpdatesTableExists() {
        return sessionContext.getSession().getCluster().getMetadata().getKeyspace(keyspace).getTable(SCHEMA_UPDATES_TABLE) != null;
    }

    private boolean allMigrationFilesApplied() {
        List<String> filesNotApplied = new ArrayList<>();
        paths.applyInSortedOrder((filename, path) -> {
            if (schemaChecker.alreadyApplied(filename)) {
                if (schemaChecker.contentsAreDifferent(filename, path)) {
                    LOGGER.error("Contents have changed: {}", path.getFileName());
                    throw new IllegalStateException("Pre-migration check detected that contents have changed for " + filename + " at " + path);
                } else {
                    LOGGER.info("Already applied: {}, skipping", path.getFileName());
                }
            } else {
                filesNotApplied.add(filename);
            }
        });

        LOGGER.info("Found {} files to be applied", filesNotApplied.size());
        return filesNotApplied.isEmpty();
    }
}
