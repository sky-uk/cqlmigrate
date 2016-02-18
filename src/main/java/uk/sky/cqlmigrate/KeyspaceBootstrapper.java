package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class KeyspaceBootstrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyspaceBootstrapper.class);

    private final ExecutionInfo executionInfo;
    private final String keyspace;
    private final CqlPaths paths;

    KeyspaceBootstrapper(ExecutionInfo executionInfo, String keyspace, CqlPaths paths) {
        this.executionInfo = executionInfo;
        this.keyspace = keyspace;
        this.paths = paths;
    }

    void bootstrap() {
        Session session = executionInfo.getSession();
        KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            ConsistencyLevel writeConsistencyLevel = executionInfo.getWriteConsistencyLevel();
            paths.applyBootstrap((filename, path) -> {
                LOGGER.info("Keyspace not found, applying {} at consistency level {}", path, writeConsistencyLevel);
                List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(path);
                CqlLoader.load(session, cqlStatements, writeConsistencyLevel);
                LOGGER.info("Applied: bootstrap.cql");
            });
        } else {
            LOGGER.info("Keyspace found, not applying bootstrap.cql");
        }
    }

}
