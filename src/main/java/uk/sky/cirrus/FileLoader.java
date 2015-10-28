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

class FileLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

    private FileLoader() {
    }

    static void loadCql(Session session, Path cqlPath) {

        final StringBuilder statementBuilder = new StringBuilder();

        try (BufferedReader cqlReader = Files.newBufferedReader(cqlPath, Charsets.UTF_8)) {

            final String cqlStatements = cqlReader.lines().map(statementBuilder::append).reduce(statementBuilder, (sb1, sb2) -> sb1).toString();
            checkState(cqlStatements.endsWith(";"), "had a non-terminated cql line: %s", cqlStatements);

            Arrays.stream(cqlStatements.split(";")).forEach(session::execute);
        } catch (Throwable t) {
            LOGGER.error("Failed to execute cql script {}: {}", cqlPath.getFileName(), t.getMessage());
            throw Throwables.propagate(t);
        }
    }
}
