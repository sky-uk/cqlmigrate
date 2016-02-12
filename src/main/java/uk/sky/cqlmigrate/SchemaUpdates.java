package uk.sky.cqlmigrate;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkNotNull;

class SchemaUpdates {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUpdates.class);
    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";
    private static final String CHECKSUM_COLUMN = "checksum";

    private final ExecutionInfo executionInfo;
    private final String keyspace;

    SchemaUpdates(ExecutionInfo executionInfo, String keyspace) {
        this.executionInfo = executionInfo;
        this.keyspace = keyspace;
    }

    void initialise() {
        Session session = executionInfo.getSession();
        session.execute(new SimpleStatement("USE " + keyspace + ";").setConsistencyLevel(executionInfo.getReadConsistencyLevel()));
        session.execute(new SimpleStatement("CREATE TABLE IF NOT EXISTS " + SCHEMA_UPDATES_TABLE + " (filename text primary key, " + CHECKSUM_COLUMN + " text, applied_on timestamp);")
                .setConsistencyLevel(executionInfo.getWriteConsistencyLevel()));
    }

    boolean alreadyApplied(String filename) {
        Row row = getSchemaUpdate(executionInfo.getSession(), filename);
        return row != null;
    }

    @Nullable
    private Row getSchemaUpdate(Session session, String filename) {
        return session.execute(
                new SimpleStatement("SELECT * FROM " + SCHEMA_UPDATES_TABLE + " where filename = ?", filename)
                        .setConsistencyLevel(executionInfo.getReadConsistencyLevel()))
                .one();
    }

    boolean contentsAreDifferent(String filename, Path path) {
        Row row = checkNotNull(getSchemaUpdate(executionInfo.getSession(), filename));
        String previousSha1 = row.getString(CHECKSUM_COLUMN);

        try {
            String checksum = calculateChecksum(path);
            return !previousSha1.equals(checksum);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    void add(String filename, Path path) {

        String query = "INSERT INTO " + SCHEMA_UPDATES_TABLE + " (filename, " + CHECKSUM_COLUMN + ", applied_on)" +
                " VALUES (?, ?, dateof(now()));";

        Statement statement = new SimpleStatement(query, filename, calculateChecksum(path)).setConsistencyLevel(executionInfo.getWriteConsistencyLevel());

        LOGGER.debug("Applying schema cql: {} path: {}", query, path);
        executionInfo.getSession().execute(statement);
    }

    private String calculateChecksum(Path path) {
        try {
            return new PathByteSource(path).hash(Hashing.sha1()).toString();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class PathByteSource extends ByteSource {
        private final Path path;

        public PathByteSource(Path path) {
            this.path = path;
        }

        @Override
        public InputStream openStream() throws IOException {
            return java.nio.file.Files.newInputStream(path);
        }
    }
}
