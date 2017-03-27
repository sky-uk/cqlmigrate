package uk.sky.cqlmigrate;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

class CqlFileParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlFileParser.class);

    private CqlFileParser() {}

    private static final char CQL_STATEMENT_STRING_DELIMITER = '\'';
    private static final String CQL_STATEMENT_TERMINATOR = ";";
    private static final String CQL_COMMENT_DOUBLE_HYPEN = "--"; //Double hypen
    private static final String CQL_MULTI_LINE_COMMENT_OPEN = "/*"; //Forward slash asterisk
    private static final String CQL_MULTI_LINE_COMMENT_CLOSE = "*/"; //Asterisk forward slash
    private static final Pattern CQL_MULTI_LINE_COMMENT_PATTERN = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    private static final Pattern EOL = Pattern.compile(".*\\R|.+\\z");
    private static final String EMPTY_STR = "";

    static List<String> getCqlStatementsFrom(Path cqlPath) {
        final LineProcessor processor = new LineProcessor();
        String original;

        try (final Scanner in = new Scanner(cqlPath, "UTF-8")) {
            while ((original = in.findWithinHorizon(EOL, 0)) != null) {
                processor.process(original);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to process cql script {}: {}", cqlPath.getFileName(), e.getMessage());
            throw Throwables.propagate(e);
        }

        processor.check();

        return Arrays.asList(processor.getResult());
    }

    static class LineProcessor {
        private static final byte INIT = 0;
        private static final byte FIND_EOS = 1;
        private static final byte IS_MULTI_LINE_COMMENT = 2;
        private static final byte IS_OPEN_STMT = 3;
        private static final byte IS_OPEN_VALUE_EXP = 4;
        private static final byte IS_CLOSE_STMT = 5;

        String[] statements;
        int index;
        byte curState = INIT;
        StringBuilder curStmt;

        void process(String original) throws IOException {
            switch (curState) {
                case INIT:
                    if (statements == null) {
                        statements = new String[1];
                    } else {
                        statements = Arrays.copyOf(statements, statements.length + 1);
                        index++;
                        statements[index] = "";
                    }
                    curState = FIND_EOS;
                    curStmt = new StringBuilder();
                    process(original);
                    break;

                case FIND_EOS:
                case IS_OPEN_STMT:
                    final String line = CharMatcher.WHITESPACE.trimFrom(original);

                    if (line.startsWith(CQL_COMMENT_DOUBLE_HYPEN) || line.isEmpty()) {
                        return;
                    }

                    if (line.startsWith(CQL_MULTI_LINE_COMMENT_OPEN)) {
                        curState = IS_MULTI_LINE_COMMENT;
                        return;
                    }

                    if (line.endsWith(CQL_STATEMENT_TERMINATOR)) {
                        curStmt.append(" ").append(line.substring(0, line.length() - 1));
                        statements[index] = CharMatcher.WHITESPACE.trimFrom(curStmt.toString());
                        curState = IS_CLOSE_STMT;
                        process(statements[index]);
                        return;
                    }

                    // A semicolon preceded by an odd number of single quotes must be within a string,
                    // and therefore is not a statement terminator
                    if (CharMatcher.is(CQL_STATEMENT_STRING_DELIMITER).countIn(line) % 2 != 0) {
                        curState = IS_OPEN_VALUE_EXP;
                        curStmt.append(" ").append(CharMatcher.WHITESPACE.trimLeadingFrom(original));
                        return;
                    }

                    final int pos = line.indexOf(CQL_COMMENT_DOUBLE_HYPEN);
                    if (pos != -1) {
                        curStmt.append(line.substring(0, pos));
                        return;
                    }

                    final Matcher matcher = CQL_MULTI_LINE_COMMENT_PATTERN.matcher(line);
                    if (matcher.find()) {
                        curStmt.append(matcher.replaceAll(EMPTY_STR));
                        return;
                    }

                    if (curState == IS_OPEN_STMT) {
                        curStmt.append(" ").append(line);
                    } else {
                        curState = IS_OPEN_STMT;
                        curStmt.append(line);
                    }

                    break;

                case IS_OPEN_VALUE_EXP:
                    if (CharMatcher.is(CQL_STATEMENT_STRING_DELIMITER).countIn(original) % 2 != 0) {
                        curStmt.append(original);
                        curState = FIND_EOS;
                        return;
                    }

                    curStmt.append(original);

                    break;

                case IS_MULTI_LINE_COMMENT:
                    if (CharMatcher.WHITESPACE.trimTrailingFrom(original).endsWith(CQL_MULTI_LINE_COMMENT_CLOSE))
                        curState = FIND_EOS;

                    break;

                case IS_CLOSE_STMT:
                    LOGGER.trace("CQL parsed: {}", original);
                    curState = INIT;
                    break;
            }
        }

        private void check() {
            checkState(curState == IS_CLOSE_STMT || curState == INIT, "File had a non-terminated cql line");
        }
        
        String[] getResult() {
            return statements;
        }
    }
}
