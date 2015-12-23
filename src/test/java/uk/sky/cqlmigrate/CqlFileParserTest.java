package uk.sky.cqlmigrate;

import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

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
}
