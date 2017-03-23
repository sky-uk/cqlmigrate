package uk.sky.cqlmigrate;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

class CqlFileParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlFileParser.class);

    private CqlFileParser() {}

    private static final char CQL_STATEMENT_STRING_DELIMITER = '\'';
    private static final String CQL_STATEMENT_TERMINATOR = ";";
    private static final String CQL_COMMENT_DOUBLE_HYPEN = "--"; //Double hypen
    private static final Pattern CQL_MULTI_LINE_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    private static final String EMPTY_STR = "";

    static List<String> getCqlStatementsFrom(Path cqlPath) {
        String cqlFileAsString;

        try (BufferedReader cqlReader = Files.newBufferedReader(cqlPath, Charsets.UTF_8)) {
            StringBuilder stringBuilder = new StringBuilder();
            cqlReader.lines()
                    .filter(line -> {
                        final String trimmed = line.trim();
                        return !trimmed.startsWith(CQL_COMMENT_DOUBLE_HYPEN);
                    })
                    .map(line -> {
                        final int pos = line.indexOf(CQL_COMMENT_DOUBLE_HYPEN);
                        if (pos != -1)
                            return line.substring(0, pos);
                        else
                            return line;
                    })
                    .forEach(stringBuilder::append);

            cqlFileAsString = CQL_MULTI_LINE_COMMENT.matcher(stringBuilder)
                    .replaceAll(EMPTY_STR);

        } catch (IOException e) {
            LOGGER.error("Failed to execute cql script {}: {}", cqlPath.getFileName(), e.getMessage());
            throw Throwables.propagate(e);
        }

        checkState(cqlFileAsString.endsWith(CQL_STATEMENT_TERMINATOR), "had a non-terminated cql line: %s", cqlFileAsString);
        return splitByStatementTerminator(cqlFileAsString);
    }

    private static List<String> splitByStatementTerminator(String cqlStatements) {
        List<String> statementList = new ArrayList<>();
        String candidateStatement = "";

        for (String statementFragment : cqlStatements.split(CQL_STATEMENT_TERMINATOR)) {
            candidateStatement += statementFragment;
            // A semicolon preceded by an odd number of single quotes must be within a string, and therefore is not a statement terminator
            if (CharMatcher.is(CQL_STATEMENT_STRING_DELIMITER).countIn(candidateStatement) % 2 == 0) {
                statementList.add(candidateStatement);
                candidateStatement = "";
            } else {
                candidateStatement += CQL_STATEMENT_TERMINATOR;
            }
        }
        return statementList;
    }
}
