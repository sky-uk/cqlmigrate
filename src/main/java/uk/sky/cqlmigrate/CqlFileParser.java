package uk.sky.cqlmigrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CqlFileParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlFileParser.class);
    private static final Pattern EOL = Pattern.compile(".*\\R|.+\\z");

    private CqlFileParser() {}

    static List<String> getCqlStatementsFrom(Path cqlPath) {
        LineProcessor processor = new LineProcessor();

        try (Scanner in = new Scanner(cqlPath, StandardCharsets.UTF_8.name())) {
            String original;
            while ((original = in.findWithinHorizon(EOL, 0)) != null) {
                processor.process(original);
            }
        } catch (IOException | IllegalStateException e ) {
            LOGGER.error("Failed to process cql script {}: {}", cqlPath.getFileName(), e.getMessage());
            throw new RuntimeException(e);
        }

        processor.check();

        return processor.getResult();
    }

    private static class LineProcessor {
        private static final char CQL_STATEMENT_STRING_DELIMITER = '\'';
        private static final String CQL_STATEMENT_TERMINATOR = ";";
        private static final String CQL_COMMENT_DOUBLE_HYPEN = "--";
        private static final String CQL_COMMENT_DOUBLE_SLASH = "//";
        private static final String CQL_MULTI_LINE_COMMENT_OPEN = "/*";
        private static final String CQL_MULTI_LINE_COMMENT_CLOSE = "*/";
        private static final Pattern CQL_MULTI_LINE_COMMENT_PATTERN = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
        private static final String EMPTY_STR = "";

        private enum State {
            INIT,
            FIND_EOS,
            IS_MULTI_LINE_COMMENT,
            IS_OPEN_STMT,
            IS_OPEN_VALUE_EXP,
            IS_CLOSE_STMT
        }

        private final List<String> statements = new ArrayList<>();
        private State curState = State.INIT;
        private StringBuilder curStmt;

        void process(String original) throws IOException {
            switch (curState) {
                case INIT:
                    init(original);

                    break;

                case FIND_EOS:
                case IS_OPEN_STMT:
                    findStatement(original);

                    break;

                case IS_OPEN_VALUE_EXP:
                    findValueExpression(original);

                    break;

                case IS_MULTI_LINE_COMMENT:
                    findMultilineComment(original);

                    break;

                case IS_CLOSE_STMT:
                    closedStatement(original);

                    break;
            }
        }

        private void init(String original) throws IOException {
            curState = State.FIND_EOS;
            curStmt = new StringBuilder();
            process(original);
        }

        private void findStatement(String original) throws IOException {
            String line = original.trim();

            if (line.startsWith(CQL_COMMENT_DOUBLE_HYPEN) || line.startsWith(CQL_COMMENT_DOUBLE_SLASH) || line.isEmpty()) {
                return;
            }

            if (line.startsWith(CQL_MULTI_LINE_COMMENT_OPEN)) {
                curState = State.IS_MULTI_LINE_COMMENT;
                return;
            }

            if (line.endsWith(CQL_STATEMENT_TERMINATOR)) {
                curStmt.append(" ").append(line, 0, line.length() - 1);
                statements.add(curStmt.toString().trim());
                curState = State.IS_CLOSE_STMT;
                process(original);
                return;
            }

            // A semicolon preceded by an odd number of single quotes must be within a string,
            // and therefore is not a statement terminator
            if (line.chars().filter(character -> character == CQL_STATEMENT_STRING_DELIMITER).count() % 2 != 0) {
                curState = State.IS_OPEN_VALUE_EXP;
                curStmt.append(" ").append(trimLeadingWhitespace(original));
                return;
            }

            int pos = line.indexOf(CQL_COMMENT_DOUBLE_HYPEN);
            if (pos != -1) {
                curStmt.append(line, 0, pos);
                return;
            }

            Matcher matcher = CQL_MULTI_LINE_COMMENT_PATTERN.matcher(line);
            if (matcher.find()) {
                curStmt.append(matcher.replaceAll(EMPTY_STR));
                return;
            }

            if (State.IS_OPEN_STMT.equals(curState)) {
                curStmt.append(" ").append(line);
            } else {
                curState = State.IS_OPEN_STMT;
                curStmt.append(line);
            }
        }

        private static String trimLeadingWhitespace(String original) {
            return original.replaceAll("^\\s+", "");
        }

        private static String trimTrailingWhitespace(String original) {
            return original.replaceAll("\\s+$", "");
        }

        private void findValueExpression(String original) {
            if (original.chars().filter(character -> character == CQL_STATEMENT_STRING_DELIMITER).count() % 2 != 0) {
                curStmt.append(original);
                curState = State.FIND_EOS;
                return;
            }

            curStmt.append(original);
        }

        private void findMultilineComment(String original) {
            if (trimTrailingWhitespace(original).endsWith(CQL_MULTI_LINE_COMMENT_CLOSE)) {
                curState = State.FIND_EOS;
            }
        }

        private void closedStatement(String original) {
            LOGGER.trace("CQL parsed: {}", original);
            curState = State.INIT;
        }

        private void check() {
            if (!(State.IS_CLOSE_STMT.equals(curState) || State.INIT.equals(curState) || (State.FIND_EOS.equals(curState) && curStmt.length() ==0))) {
                throw new IllegalStateException("File had a non-terminated cql line");
            }
        }

        List<String> getResult() {
            return statements;
        }
    }
}
