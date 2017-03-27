package uk.sky.cqlmigrate;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
        assertThat(cqlStatements, hasSize(1));
        assertThat(cqlStatements.get(0), equalToIgnoringWhiteSpace(expectedStatement));
    }

    @Test
    public void shouldIgnoreCommentsAndNormolizeAndPreserveNewlineInValuesExp() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_rolegraphs_one/2015-08-16-12:05-statement-with-comments.cql");

        //when
        List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);
        //cqlStatements.forEach(s -> System.out.println("CQL:" + s + "\n"));

        //then
        String expectedStatement;
        assertThat(cqlStatements, hasSize(4));

        expectedStatement = "INSERT into role_graphs (provider, graphml)\n" +
                "  VALUES ('SKY', 'some text; some more text')";

        assertThat(cqlStatements.get(0), equalToIgnoringWhiteSpace(expectedStatement));

        expectedStatement = "CREATE TABLE role_graphs_sql(" +
                "provider text, " +
                "graphml text, " +
                "settings text, " +
                "PRIMARY KEY (provider)" +
                ") WITH comment='test table role_graphs_sql'";

        assertThat(cqlStatements.get(1), equalToIgnoringWhiteSpace(expectedStatement));

        expectedStatement = "INSERT into role_graphs_sql (provider, graphml)" +
                " VALUES ('SKY', 'some ...  \n" +
                "-- it''s comment\n" +
                "-- Created by yEd 3.12.2 /* <key for=\"graphml\" id=\"d0\" yfiles.type=\"resources\"/> */ <test>''</test>\n" +
                "   the end ')";
        
        assertEquals(cqlStatements.get(2), expectedStatement);

        expectedStatement = "INSERT into role_graphs_sql (provider, graphml, settings)" +
                " VALUES ('EARTH', '', '   the end ')";

        assertEquals(cqlStatements.get(3), expectedStatement);
    }

    @Test
    public void shouldExceptionOnMissingSemicolon() throws Exception {
        Path cqlPath = getResourcePath("cql_bootstrap_missing_semicolon/bootstrap.cql");
        try {
            List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);
            fail();
        } catch (IllegalStateException e) {
            //ok
        }
    }

    @Test
    public void shouldCopeWithSingleLineStatement() throws Exception {
        Path cqlPath = getResourcePath("cql_consistency_level/2016-02-12-11_30-create-table.cql");

        List<String> cqlStatements = CqlFileParser.getCqlStatementsFrom(cqlPath);
        String expectedStatement = "CREATE TABLE consistency_test (column1 text primary key, column2 text)";
        assertThat(cqlStatements, hasSize(1));
        assertEquals(cqlStatements.get(0), expectedStatement);
    }
}
