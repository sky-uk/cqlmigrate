package uk.sky.cirrus;

import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

class SchemaLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoader.class);

    private final Session session;
    private final String keyspace;
    private final SchemaUpdates schemaUpdates;
    private final Paths paths;

    public SchemaLoader(Session session, String keyspace, SchemaUpdates schemaUpdates, Paths paths) {
        this.session = session;
        this.keyspace = keyspace;
        this.schemaUpdates = schemaUpdates;
        this.paths = paths;
    }

    public void load() {
        session.execute("USE " + keyspace + ";");
        paths.applyInSortedOrder(new Loader());
    }

    private class Loader implements Paths.Function {
        @Override
        public void apply(String filename, Path path) {
            if (schemaUpdates.alreadyApplied(filename)) {
                if (schemaUpdates.contentsAreDifferent(filename, path)) {
                    LOGGER.error("Contents have changed: {}", path.getFileName());
                    throw new IllegalStateException("Contents have changed for " + filename + " at " + path);
                } else {
                    LOGGER.info("Skipped: {}", path.getFileName());
                }
            } else {
                String lowercasePath = path.toString().toLowerCase();
                if (lowercasePath.endsWith(".cql")) {
                    FileLoader.loadCql(session, path);
                } else {
                    throw new IllegalArgumentException("Unrecognised file type: " + path);
                }

                schemaUpdates.add(filename, path);
                LOGGER.info("Applied: {}", path.getFileName());

            }
        }
    }
}
