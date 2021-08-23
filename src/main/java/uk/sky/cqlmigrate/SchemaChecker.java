package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.nio.file.Path;

import static com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance;
import static java.util.Objects.requireNonNull;

class SchemaChecker {
    public static final String SCHEMA_UPDATES_TABLE = "schema_updates";
    private static final String CHECKSUM_COLUMN = "checksum";

    private final SessionContext sessionContext;
    private final String keyspace;

    SchemaChecker(SessionContext sessionContext, String keyspace) {
        this.sessionContext = sessionContext;
        this.keyspace = keyspace;
    }

    boolean alreadyApplied(String filename) {
        Row row = getSchemaUpdate(sessionContext.getSession(), filename);
        return row != null;
    }

    private Row getSchemaUpdate(CqlSession session, String filename) {

        return session.execute(newInstance("SELECT * FROM " + keyspace + "." + SCHEMA_UPDATES_TABLE + " where filename = ?", filename)
                .setConsistencyLevel(sessionContext.getReadConsistencyLevel())).one();
    }

    boolean contentsAreDifferent(String filename, Path path) {
        Row row = requireNonNull(getSchemaUpdate(sessionContext.getSession(), filename));
        String previousSha1 = row.getString(CHECKSUM_COLUMN);

        try {
            String checksum = ChecksumCalculator.calculateChecksum(path);
            return !previousSha1.equals(checksum);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
