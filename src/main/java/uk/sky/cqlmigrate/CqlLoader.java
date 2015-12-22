package uk.sky.cqlmigrate;

import com.datastax.driver.core.Session;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

class CqlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlLoader.class);

    private static final String CQL_STATEMENT_STRING_DELIMITER = "'";
    private static final String CQL_STATEMENT_TERMINATOR = ";";

    private CqlLoader() {}

    static void load(Session session, Path cqlPath) {
        StringBuilder statementBuilder = new StringBuilder();

        try (BufferedReader cqlReader = Files.newBufferedReader(cqlPath, Charsets.UTF_8)) {

            cqlReader.lines().forEach(statementBuilder::append);

            String cqlStatements = statementBuilder.toString();
            checkState(cqlStatements.endsWith(CQL_STATEMENT_TERMINATOR), "had a non-terminated cql line: %s", cqlStatements);

            splitByStatementTerminator(cqlStatements).stream().forEach(statement -> {
                LOGGER.debug("Executing cql statement {}", statement);
                session.execute(statement);
            });
        } catch (Throwable t) {
            LOGGER.error("Failed to execute cql script {}: {}", cqlPath.getFileName(), t.getMessage());
            throw Throwables.propagate(t);
        }
    }

    private static List<String> splitByStatementTerminator(String cqlStatements) {
        List<String> statementList = new ArrayList<>();
        String candidateStatement = "";

        for (String statementFragment : cqlStatements.split(CQL_STATEMENT_TERMINATOR)) {
            candidateStatement += statementFragment;
            // A semicolon preceded by an odd number of single quotes must be within a string, and therefore is not a statement terminator
            if (CharMatcher.is('\'').countIn(candidateStatement) % 2 == 0) {
                statementList.add(candidateStatement);
                candidateStatement = "";
            } else {
                candidateStatement += CQL_STATEMENT_TERMINATOR;
            }
        }
        return statementList;
    }
}