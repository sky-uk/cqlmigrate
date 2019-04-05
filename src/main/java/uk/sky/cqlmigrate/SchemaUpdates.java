package uk.sky.cqlmigrate;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

class SchemaUpdates {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUpdates.class);
    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";
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

    boolean alreadyApplied(String filename) {
        Row row = getSchemaUpdate(sessionContext.getSession(), filename);
        return row != null;
    }

    private Row getSchemaUpdate(Session session, String filename) {
        return session.execute(
                new SimpleStatement("SELECT * FROM " + SCHEMA_UPDATES_TABLE + " where filename = ?", filename)
                        .setConsistencyLevel(sessionContext.getReadConsistencyLevel()))
                .one();
    }

    boolean contentsAreDifferent(String filename, Path path) {
        Row row = requireNonNull(getSchemaUpdate(sessionContext.getSession(), filename));
        String previousSha1 = row.getString(CHECKSUM_COLUMN);

        try {
            String checksum = calculateChecksum(path);
            return !previousSha1.equals(checksum);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void add(String filename, Path path) {

        String query = "INSERT INTO " + SCHEMA_UPDATES_TABLE + " (filename, " + CHECKSUM_COLUMN + ", applied_on)" +
                " VALUES (?, ?, dateof(now()));";

        Statement statement = new SimpleStatement(query, filename, calculateChecksum(path)).setConsistencyLevel(sessionContext.getWriteConsistencyLevel());

        LOGGER.debug("Applying schema cql: {} path: {}", query, path);
        sessionContext.getSession().execute(statement);
    }

    private String calculateChecksum(Path path) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            final byte[] hash = digest.digest(Files.readAllBytes(path));
            return bytesToHex(hash);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        final StringBuilder builder = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            final int asUnsigned = Byte.toUnsignedInt(b);
            builder.append(Character.forDigit(asUnsigned >>> 4, 16))
                   .append(Character.forDigit(asUnsigned & 0x0F, 16));
        }
        return builder.toString();
    }
}
