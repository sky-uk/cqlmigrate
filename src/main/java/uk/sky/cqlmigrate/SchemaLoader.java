package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

class SchemaLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoader.class);

    private final SessionContext sessionContext;
    private final String keyspace;
    private final SchemaUpdates schemaUpdates;
    private final SchemaChecker schemaChecker;

    private final TableChecker tableChecker;
    private final CqlPaths paths;

    SchemaLoader(SessionContext sessionContext, String keyspace, SchemaUpdates schemaUpdates,
                 SchemaChecker schemaChecker, TableChecker tableChecker, CqlPaths paths) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
        this.schemaUpdates = schemaUpdates;
        this.schemaChecker = schemaChecker;
        this.tableChecker = tableChecker;
        this.paths = paths;
    }

    void load() {
        sessionContext.getSession().execute(SimpleStatement.newInstance("USE " + keyspace + ";").setConsistencyLevel(sessionContext.getReadConsistencyLevel()));
        paths.applyInSortedOrder(new Loader());
    }

    private class Loader implements CqlPaths.Function {
        @Override
        public void apply(String filename, Path path) {
            if (schemaChecker.alreadyApplied(filename)) {
                if (schemaChecker.contentsAreDifferent(filename, path)) {
                    LOGGER.error("Contents have changed: {}", path.getFileName());
                    throw new IllegalStateException("Contents have changed for " + filename + " at " + path);
                } else {
                    LOGGER.info("Skipped: {}", path.getFileName());
                }
            } else {
                String lowercasePath = path.toString().toLowerCase();
                if (lowercasePath.endsWith(".cql")) {
                    List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(path);
                    CqlLoader.load(sessionContext, cqlStatements);
                    tableChecker.check(sessionContext.getSession(), keyspace);
                } else {
                    throw new IllegalArgumentException("Unrecognised file type: " + path);
                }

                schemaUpdates.add(filename, path);
                LOGGER.info("Applied: {}", path.getFileName());
            }
        }
    }
}
