package uk.sky.cirrus;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
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

    private final Session session;
    private final String keyspace;

    SchemaUpdates(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    void initialise() {
        session.execute("USE " + keyspace + ";");
        KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata.getTable(SCHEMA_UPDATES_TABLE) == null) {
            session.execute("CREATE TABLE " + SCHEMA_UPDATES_TABLE + " (filename text primary key, " + CHECKSUM_COLUMN + " text, applied_on timestamp);");
        }
    }

    boolean alreadyApplied(String filename) {
        Row row = getSchemaUpdate(session, filename);
        return row != null;
    }

    @Nullable
    private Row getSchemaUpdate(Session session, String filename) {
        return session.execute("SELECT * FROM " + SCHEMA_UPDATES_TABLE + " where filename = ?", filename)
                      .one();
    }

    boolean contentsAreDifferent(String filename, Path path) {
        Row row = checkNotNull(getSchemaUpdate(session, filename));
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
        LOGGER.debug("Applying schema cql: {} path: {}", query, path);
        session.execute(query, filename, calculateChecksum(path));
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
