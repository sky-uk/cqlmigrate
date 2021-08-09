package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
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
        CqlSession session = sessionContext.getSession();
        session.execute(SimpleStatement
                .newInstance("USE " + keyspace + ";")
                .setConsistencyLevel(sessionContext.getReadConsistencyLevel())
        );
        TableMetadata schemaUpdateTableMetadata = session
                .getMetadata()
                .getKeyspace(keyspace)
                .flatMap(k -> k.getTable(SCHEMA_UPDATES_TABLE)).orElse(null);

        if (schemaUpdateTableMetadata == null) {
            CqlLoader.load(sessionContext,
                    Collections.singletonList("CREATE TABLE " + SCHEMA_UPDATES_TABLE + " (filename text primary key, " + CHECKSUM_COLUMN + " text, applied_on timestamp);")
            );
        }
    }

    void add(String filename, Path path) {

        String query = "INSERT INTO " + SCHEMA_UPDATES_TABLE + " (filename, " + CHECKSUM_COLUMN + ", applied_on)" +
                " VALUES (?, ?, dateof(now()));";

        Statement statement = SimpleStatement.newInstance(query, filename, ChecksumCalculator.calculateChecksum(path)).setConsistencyLevel(sessionContext.getWriteConsistencyLevel());

        LOGGER.debug("Applying schema cql: {} path: {}", query, path);
        sessionContext.getSession().execute(statement);
    }
}
