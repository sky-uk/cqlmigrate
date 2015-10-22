package uk.sky.cirrus;

import com.datastax.driver.core.*;
import org.junit.*;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

public class CqlMigratorTest {

    private static final Collection<String> CASSANDRA_HOSTS = singletonList("localhost");
    private static final Cluster cluster = Cluster.builder().addContactPoints(CASSANDRA_HOSTS.toArray(new String[CASSANDRA_HOSTS.size()])).build();
    private static final Session session = cluster.connect();
    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String LOCK_NAME = TEST_KEYSPACE + ".schema_migration";
    private static final CqlMigrator migrator = new CqlMigrator();

    @BeforeClass
    public static void setUpClass() throws Exception {
        session.execute("CREATE KEYSPACE locks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE locks.locks (name text PRIMARY KEY, client uuid)");
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
    }

    @After
    public void tearDown() {
        session.execute("TRUNCATE locks.locks");

        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        session.execute("DROP KEYSPACE locks");
        cluster.closeAsync();
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void shouldRunTheBootstrapCqlIfKeyspaceDoesNotExist() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_bootstrap");
        Collection<Path> cqlPaths = singletonList(cqlPath);

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        try {
            cluster.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test
    public void shouldThrowCannotAcquireLockExceptionIfLockCannotBeAcquired() throws Exception {
        //given
        UUID client = UUID.randomUUID();
        session.execute("INSERT INTO locks.locks (name, client) VALUES (?, ?)", LOCK_NAME, client);

        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        Throwable throwable = catchThrowable(() -> migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths));

        //then
        assertThat(throwable).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getMessage()).isEqualTo("Lock currently in use by client: " + client);
    }

    @Test
    public void shouldMigrateSchemaIfLockCanBeAcquired() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        try {
            cluster.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test
    public void shouldRemoveLockAfterMigration() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        ResultSet resultSet = session.execute("SELECT * FROM locks.locks WHERE name = ?", LOCK_NAME);
        assertThat(resultSet.isExhausted()).as("Is lock released").isTrue();
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForInvalidBootstrap() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_invalid_bootstrap");
        Collection<Path> cqlPaths = singletonList(cqlPath);

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForBootstrapWithMissingSemiColon() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_bootstrap_missing_semicolon");
        Collection<Path> cqlPaths = singletonList(cqlPath);

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenMultipleBootstrap() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_bootstrap_duplicate"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test
    public void shouldLoadCqlFilesInOrderAcrossDirectories() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from status where dependency = 'developers'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("waste_of_space")).isEqualTo("true");
    }

    @Test
    public void canAddNewFilesWhenOldFilesHaveAlreadyBeenApplied() throws Exception {
        //given
        Collection<Path> cqlPaths = new ArrayList<>();
        cqlPaths.add(getResourcePath("cql_valid_one"));
        cqlPaths.add(getResourcePath("cql_valid_two"));
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //when
        cqlPaths.add(getResourcePath("cql_valid_three"));
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from status where dependency = 'developers'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("waste_of_space")).isEqualTo("false");
    }

    @Test
    public void schemaUpdatesTableShouldContainTheDateEachFileWasApplied() throws Exception {
        //given
        Date now = new Date();
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from schema_updates");
        for (Row row : rs) {
            assertThat(row.getDate("applied_on")).as("applied_on").isNotNull().isAfter(now);
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailIfThereAreDuplicateCqlFilenames() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"),
                getResourcePath("cql_valid_duplicate_filename"));

        //when
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);
    }

    @Test
    public void shouldNotLoadAnyOfTheCqlFilesIfThereAreDuplicateCqlFilenames() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"),
                getResourcePath("cql_valid_duplicate_filename"));

        //when
        try {
            migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);
            fail("Should have died");
        } catch (RuntimeException e) {
            // nada
        }

        //then
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata).as("should not have made any schema changes").isNull();
    }

    @Test
    public void shouldFailWhenFileContentsChangeForAPreviouslyAppliedCqlFile() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_create_status"));
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //when
        try {
            Collection<Path> differentContentsPaths = singletonList(getResourcePath("cql_create_status_different_contents"));
            migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, differentContentsPaths);
            fail("Should have died");
        } catch (RuntimeException e) {
            // nada
        }

        //then
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata.getTable("another_status")).as("table should not have been created").isNull();
    }

    @Test
    public void shouldNotLoadAnyCqlFilesInSubDirectories() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_sub_directories"));

        //when
        try {
            migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);
        } catch (RuntimeException e) {
            // nada
        }

        //then
        TableMetadata tableMetadata = cluster.getMetadata().getKeyspace(TEST_KEYSPACE).getTable("status");
        assertThat(tableMetadata.getColumn("waste_of_space"))
                .as("should not have made any schema changes from sub directory")
                .isNull();
    }

    @Test
    public void shouldLoadCqlFilesInOrderAcrossDirectoriesForMain() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("keyspace", TEST_KEYSPACE);
        System.setProperty("directories", getResourcePath("cql_valid_one").toString() + "," + getResourcePath("cql_valid_two").toString());

        //when
        CqlMigrator.main(new String[]{});

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from status where dependency = 'developers'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("waste_of_space")).isEqualTo("true");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentIfHostsNotSetForMain() throws Exception {
        //given
        System.setProperty("keyspace", TEST_KEYSPACE);
        System.setProperty("directories", getResourcePath("cql_valid_one").toString() + "," + getResourcePath("cql_valid_two").toString());

        //when
        CqlMigrator.main(new String[]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentIfKeyspaceNotSetForMain() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("directories", getResourcePath("cql_valid_one").toString() + "," + getResourcePath("cql_valid_two").toString());

        //when
        CqlMigrator.main(new String[]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentIfDirectoriesNotSetForMain() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("keyspace", TEST_KEYSPACE);

        //when
        CqlMigrator.main(new String[]{});
    }

    @Test
    public void shouldRemoveAllDataWhenCleaningAKeyspace() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));
        migrator.migrate(CASSANDRA_HOSTS, TEST_KEYSPACE, cqlPaths);

        //when
        migrator.clean(CASSANDRA_HOSTS, TEST_KEYSPACE);

        //then
        assertThat(cluster.getMetadata().getKeyspace(TEST_KEYSPACE)).as("keyspace should be gone").isNull();
    }

    @Test
    public void shouldFailSilentlyIfCleaningANonExistingKeyspace() throws Exception {
        migrator.clean(CASSANDRA_HOSTS, TEST_KEYSPACE);
    }

}
