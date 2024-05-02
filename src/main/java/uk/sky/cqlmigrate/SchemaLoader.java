package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;

class SchemaLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoader.class);

    private final SessionContext sessionContext;
    private final String keyspace;
    private final SchemaUpdates schemaUpdates;
    private final SchemaChecker schemaChecker;
    private final CqlPaths paths;
    private final Duration awskTableUpdateTimeout;

    SchemaLoader(SessionContext sessionContext, String keyspace, SchemaUpdates schemaUpdates,
                 SchemaChecker schemaChecker, CqlPaths paths, Duration awskTableUpdateTimeout) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
        this.schemaUpdates = schemaUpdates;
        this.schemaChecker = schemaChecker;
        this.paths = paths;
        this.awskTableUpdateTimeout = awskTableUpdateTimeout;
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
                } else {
                    throw new IllegalArgumentException("Unrecognised file type: " + path);
                }

                schemaUpdates.add(filename, path);

                LOGGER.info("Applied: {}", path.getFileName());

                if (nonNull(awskTableUpdateTimeout)) {
                    LOGGER.info("Waiting for tables to be provisioned");
                    Awaitility.await()
                            .pollInterval(10, TimeUnit.SECONDS)
                            .atMost(awskTableUpdateTimeout)
                            .until(() -> {
                                try {
                                    ResultSet result = sessionContext.getSession().execute("SELECT table_name, status FROM system_schema_mcs.tables WHERE keyspace_name=?", keyspace);
                                    return !result.all().stream()
                                            .filter(row -> !"ACTIVE".equals(row.getString("status")))
                                            .peek(row -> LOGGER.info("Table {} is still in {} state", row.getString("table_name"), row.getString("status")))
                                            .findAny()
                                            .isPresent();
                                } catch (Exception ex) {
                                    return false;
                                }
                            });
                    LOGGER.info("All tables are ready");
                }

            }
        }
    }
}