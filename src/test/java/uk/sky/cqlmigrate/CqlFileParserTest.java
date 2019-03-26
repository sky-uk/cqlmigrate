package uk.sky.cqlmigrate;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CqlFileParserTest {

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void shouldCopeWithSemicolonsInStrings() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_rolegraphs_one/2015-08-16-12:00-semicolon-in-string.cql");

        //when
        List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);

        //then
        String expectedStatement = "INSERT into role_graphs (provider, graphml) VALUES ('SKY', 'some text; some more text')";
        assertThat(cqlStatements)
            .hasOnlyOneElementSatisfying(cqlStatement ->
                                             assertThat(cqlStatement)
                                                 .isEqualToIgnoringCase(expectedStatement)
            );
    }

    @Test
    public void shouldIgnoreCommentsAndNormalizeAndPreserveNewlineInValuesExp() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_rolegraphs_one/2015-08-16-12:05-statement-with-comments.cql");

        //when
        List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);

        //then
        String expectedStatement;
        assertThat(cqlStatements)
            .hasSize(4);

        expectedStatement = "INSERT into role_graphs (provider, graphml)\n" +
                "  VALUES ('SKY', 'some text; some more text')";
        assertThat(cqlStatements.get(0))
            .isEqualToIgnoringWhitespace(expectedStatement);

        expectedStatement = "CREATE TABLE role_graphs_sql(" +
                "provider text, " +
                "graphml text, " +
                "settings text, " +
                "PRIMARY KEY (provider)" +
                ") WITH comment='test table role_graphs_sql'";

        assertThat(cqlStatements.get(1))
            .isEqualToIgnoringWhitespace(expectedStatement);

        expectedStatement = "INSERT into role_graphs_sql (provider, graphml)" +
                " VALUES ('SKY', 'some ...  \n" +
                "-- it''s comment\n" +
                "-- Created by yEd 3.12.2 /* <key for=\"graphml\" id=\"d0\" yfiles.type=\"resources\"/> */ <test>''</test>\n" +
                "   the end ')";

        assertThat(cqlStatements.get(2))
            .isEqualTo(expectedStatement);

        expectedStatement = "INSERT into role_graphs_sql (provider, graphml, settings)" +
                " VALUES ('EARTH', '', '   the end ')";

        assertThat(cqlStatements.get(3))
            .isEqualTo(expectedStatement);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldExceptionOnMissingSemicolon() throws Exception {
        Path cqlPath = getResourcePath("cql_bootstrap_missing_semicolon/bootstrap.cql");
        CqlFileParser.getCqlStatementsFrom(cqlPath);
    }

    @Test
    public void shouldCopeWithSingleLineStatement() throws Exception {
        Path cqlPath = getResourcePath("cql_consistency_level/2016-02-12-11_30-create-table.cql");

        List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);
        String expectedStatement = "CREATE TABLE consistency_test (column1 text primary key, column2 text)";
        assertThat(cqlStatements)
            .hasOnlyOneElementSatisfying(cqlStatement ->
                                             assertThat(cqlStatement)
                                                 .isEqualToIgnoringCase(expectedStatement)
            );
    }
}
