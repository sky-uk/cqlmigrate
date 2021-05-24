package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;

class SchemaUpdates {
    public static final String SCHEMA_UPDATES_TABLE = "schema_updates";

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUpdates.class);
    private static final String CHECKSUM_COLUMN = "checksum";

    private final SessionContext sessionContext;
    private final String keyspace;

    SchemaUpdates(SessionContext sessionContext, String keyspace) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
    }

    void initialise() {
        Session session = sessionContext.getSession();
        session.execute(new SimpleStatement("USE " + keyspace + ";").setConsistencyLevel(sessionContext.getReadConsistencyLevel()));
        TableMetadata schemaUpdateTableMetadata = session.getCluster().getMetadata().getKeyspace(keyspace).getTable(SCHEMA_UPDATES_TABLE);
        if (schemaUpdateTableMetadata == null) {
            CqlLoader.load(sessionContext, Collections.singletonList("CREATE TABLE " + SCHEMA_UPDATES_TABLE + " (filename text primary key, " + CHECKSUM_COLUMN + " text, applied_on timestamp);"));
        }
    }

    void add(String filename, Path path) {

        String query = "INSERT INTO " + SCHEMA_UPDATES_TABLE + " (filename, " + CHECKSUM_COLUMN + ", applied_on)" +
                " VALUES (?, ?, dateof(now()));";

        Statement statement = new SimpleStatement(query, filename, ChecksumCalculator.calculateChecksum(path)).setConsistencyLevel(sessionContext.getWriteConsistencyLevel());

        LOGGER.debug("Applying schema cql: {} path: {}", query, path);
        sessionContext.getSession().execute(statement);
    }

}
