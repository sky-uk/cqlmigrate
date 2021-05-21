package uk.sky.cqlmigrate;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

class SchemaChecker {
    public static final String SCHEMA_UPDATES_TABLE = "schema_updates";

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChecker.class);
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

    private Row getSchemaUpdate(Session session, String filename) {
        return session.execute(
                new SimpleStatement("SELECT * FROM " + keyspace + "." + SCHEMA_UPDATES_TABLE + " where filename = ?", filename)
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
