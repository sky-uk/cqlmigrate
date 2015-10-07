package uk.sky.cirrus;

import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;

class FileLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

    private FileLoader() {
    }

    static void loadCql(Session session, Path cqlPath) {
        try (BufferedReader cqlReader = Files.newBufferedReader(cqlPath, Charsets.UTF_8)) {
            String statement = "";
            while (true) {
                String line = cqlReader.readLine();
                if (line == null) break;
                statement += line;
                if (statement.endsWith(";")) {
                    session.execute(statement);
                    statement = "";
                }
            }

            checkState(statement.isEmpty(), "had a non-terminated cql line: %s", statement);

        } catch (Throwable t) {
            LOGGER.error("Failed to execute cql script {}: {}", cqlPath.getFileName(), t.getMessage());
            throw Throwables.propagate(t);
        }
    }
}
