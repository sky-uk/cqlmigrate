package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SchemaLoaderTest {

    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String SCHEMA_UPDATES_TABLE = "schema_updates";
    public static final String FILENAME = "test";

    @Mock
    private static CqlSession session;
    @Mock
    private TableChecker tableChecker;
    @Mock
    private SessionContext sessionContext;
    @Mock
    private SchemaChecker schemaChecker;
    @Mock
    private SchemaUpdates schemaUpdates;
    @Mock
    private static ClusterHealth clusterHealth;

    private CqlPaths cqlPaths;

    private SchemaLoader schemaLoader;

    @Before
    public void setUp() {
        given(sessionContext.getSession()).willReturn(session);
    }

    @Test
    public void shouldLoadCqls() {
        //given
        given(schemaChecker.alreadyApplied(FILENAME)).willReturn(false);
        cqlPaths = new CqlPaths(new HashMap<String, Path>() {{
            put(FILENAME, Paths.get(ClassLoader.getSystemResource("cql_bootstrap/bootstrap.cql").getPath()));
        }});
        schemaLoader = new SchemaLoader(sessionContext, TEST_KEYSPACE, schemaUpdates, schemaChecker, tableChecker, cqlPaths);

        //when
        schemaLoader.load();

        //then
        verify(tableChecker).check(session, TEST_KEYSPACE);
    }

    @Test
    public void shouldNotLoadCqlsWithWrongExtension() {
        //given
        given(schemaChecker.alreadyApplied(FILENAME)).willReturn(false);
        cqlPaths = new CqlPaths(new HashMap<String, Path>() {{
            put(FILENAME, Paths.get("cql_bootstrap/bootstrap.cql1"));
        }});
        schemaLoader = new SchemaLoader(sessionContext, TEST_KEYSPACE, schemaUpdates, schemaChecker, tableChecker, cqlPaths);

        //when
        Throwable throwable = catchThrowable(schemaLoader::load);

        //then
        Assertions.assertThat(throwable).isNotNull();
        Assertions.assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThat(throwable).hasMessage("Unrecognised file type: cql_bootstrap/bootstrap.cql1");
        verify(tableChecker, never()).check(session, TEST_KEYSPACE);
    }

    @Test
    public void shouldNotLoadCqlsWithWrongContent() {
        //given
        Path path = Paths.get("cql_bootstrap/bootstrap.cql");
        given(schemaChecker.alreadyApplied(FILENAME)).willReturn(true);
        given(schemaChecker.contentsAreDifferent(FILENAME, path)).willReturn(true);
        cqlPaths = new CqlPaths(new HashMap<String, Path>() {{
            put(FILENAME, path);
        }});
        schemaLoader = new SchemaLoader(sessionContext, TEST_KEYSPACE, schemaUpdates, schemaChecker, tableChecker, cqlPaths);

        //when
        Throwable throwable = catchThrowable(schemaLoader::load);

        //then
        Assertions.assertThat(throwable).isNotNull();
        Assertions.assertThat(throwable).isInstanceOf(IllegalStateException.class);
        Assertions.assertThat(throwable).hasMessage("Contents have changed for test at cql_bootstrap/bootstrap.cql");
        verify(tableChecker, never()).check(session, TEST_KEYSPACE);
    }

    @Test
    public void shouldSkipLoadCqls() {
        //given
        Path path = Paths.get("cql_bootstrap/bootstrap.cql");
        given(schemaChecker.alreadyApplied(FILENAME)).willReturn(true);
        given(schemaChecker.contentsAreDifferent(FILENAME, path)).willReturn(false);
        cqlPaths = new CqlPaths(new HashMap<String, Path>() {{
            put(FILENAME, path);
        }});
        schemaLoader = new SchemaLoader(sessionContext, TEST_KEYSPACE, schemaUpdates, schemaChecker, tableChecker, cqlPaths);

        //when
        Throwable throwable = catchThrowable(schemaLoader::load);

        //then
        Assertions.assertThat(throwable).isNull();
        ;
        verify(tableChecker, never()).check(session, TEST_KEYSPACE);
    }
}
