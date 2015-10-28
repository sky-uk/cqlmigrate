package uk.sky.cirrus;

import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;

class CqlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlLoader.class);

    private CqlLoader() {
    }

    static void load(Session session, Path cqlPath) {
        final StringBuilder statementBuilder = new StringBuilder();

        try (BufferedReader cqlReader = Files.newBufferedReader(cqlPath, Charsets.UTF_8)) {

            cqlReader.lines().forEach(statementBuilder::append);

            final String cqlStatements = statementBuilder.toString();
            checkState(cqlStatements.endsWith(";"), "had a non-terminated cql line: %s", cqlStatements);

            Arrays.stream(cqlStatements.split(";")).forEach(session::execute);
        } catch (Throwable t) {
            LOGGER.error("Failed to execute cql script {}: {}", cqlPath.getFileName(), t.getMessage());
            throw Throwables.propagate(t);
        }
    }
}