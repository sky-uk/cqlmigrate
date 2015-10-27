package uk.sky.cirrus;

import com.datastax.driver.core.*;
import org.assertj.core.api.ThrowableAssert;
import org.junit.*;
import uk.sky.cirrus.locking.LockConfig;
import uk.sky.cirrus.locking.exception.CannotAcquireLockException;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.joda.time.Duration.millis;
import static org.junit.Assert.fail;

public class CqlMigratorTest {

    private static final Collection<String> CASSANDRA_HOSTS = singletonList("localhost");
    private static final Cluster CLUSTER = Cluster.builder().addContactPoints(CASSANDRA_HOSTS.toArray(new String[CASSANDRA_HOSTS.size()])).build();
    private static final Session SESSION = CLUSTER.connect();
    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String LOCK_NAME = TEST_KEYSPACE + ".schema_migration";
    private static final int BINARY_PORT = 9042;
    private static final String REPLICATION_CLASS = "SimpleStrategy";
    private static final int REPLICATION_FACTOR = 1;

    private static final CqlMigrator MIGRATOR = new CqlMigrator();

    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        SESSION.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        executorService = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDown() {
        SESSION.execute("TRUNCATE locks.locks");
        executorService.shutdownNow();
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        SESSION.execute("DROP KEYSPACE locks");
        CLUSTER.closeAsync();
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void shouldRunTheBootstrapCqlIfKeyspaceDoesNotExist() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        try {
            CLUSTER.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test(timeout = 550)
    public void shouldThrowCannotAcquireLockExceptionIfLockCannotBeAcquiredAfterTimeout() throws Exception {
        //given
        final CqlMigrator migrator = new CqlMigrator(new LockConfig(millis(50), millis(300), REPLICATION_CLASS, REPLICATION_FACTOR));

        UUID client = UUID.randomUUID();
        SESSION.execute("INSERT INTO locks.locks (name, client) VALUES (?, ?)", LOCK_NAME, client);

        final Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        final Future<?> future = executorService.submit(new Runnable() {
            @Override
            public void run() {
                migrator.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);
            }
        });
        Thread.sleep(310);
        Throwable throwable = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                future.get();
            }
        });

        //then
        assertThat(throwable).isInstanceOf(ExecutionException.class);
        assertThat(throwable.getCause()).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause().getMessage()).isEqualTo("Lock currently in use by client: " + client);
    }

    @Test
    public void shouldMigrateSchemaIfLockCanBeAcquired() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        try {
            CLUSTER.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test
    public void shouldRemoveLockAfterMigration() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        ResultSet resultSet = SESSION.execute("SELECT * FROM locks.locks WHERE name = ?", LOCK_NAME);
        assertThat(resultSet.isExhausted()).as("Is lock released").isTrue();
    }

    @Test
    public void shouldRetryWhenAcquiringLockIfNotInitiallyAvailable() throws Exception {
        //given
        SESSION.execute("INSERT INTO locks.locks (name, client) VALUES (?, ?)", LOCK_NAME, UUID.randomUUID());
        final Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        Future<?> future = executorService.submit(new Runnable() {
            @Override
            public void run() {
                MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);
            }
        });
        SESSION.execute("TRUNCATE locks.locks");
        Thread.sleep(1000);
        future.get();

        //then
        try {
            CLUSTER.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForInvalidBootstrap() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_invalid_bootstrap");
        Collection<Path> cqlPaths = singletonList(cqlPath);

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        CLUSTER.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForBootstrapWithMissingSemiColon() throws Exception {
        //given
        Path cqlPath = getResourcePath("cql_bootstrap_missing_semicolon");
        Collection<Path> cqlPaths = singletonList(cqlPath);

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        CLUSTER.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenMultipleBootstrap() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_bootstrap_duplicate"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        CLUSTER.connect(TEST_KEYSPACE);
    }

    @Test
    public void shouldLoadCqlFilesInOrderAcrossDirectories() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = CLUSTER.connect(TEST_KEYSPACE);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //when
        cqlPaths.add(getResourcePath("cql_valid_three"));
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = CLUSTER.connect(TEST_KEYSPACE);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //then
        Session session = CLUSTER.connect(TEST_KEYSPACE);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);
    }

    @Test
    public void shouldNotLoadAnyOfTheCqlFilesIfThereAreDuplicateCqlFilenames() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"),
                getResourcePath("cql_valid_duplicate_filename"));

        //when
        try {
            MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);
            fail("Should have died");
        } catch (RuntimeException e) {
            // nada
        }

        //then
        KeyspaceMetadata keyspaceMetadata = CLUSTER.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata).as("should not have made any schema changes").isNull();
    }

    @Test
    public void shouldFailWhenFileContentsChangeForAPreviouslyAppliedCqlFile() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_create_status"));
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //when
        try {
            Collection<Path> differentContentsPaths = singletonList(getResourcePath("cql_create_status_different_contents"));
            MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, differentContentsPaths);
            fail("Should have died");
        } catch (RuntimeException e) {
            // nada
        }

        //then
        KeyspaceMetadata keyspaceMetadata = CLUSTER.getMetadata().getKeyspace(TEST_KEYSPACE);
        assertThat(keyspaceMetadata.getTable("another_status")).as("table should not have been created").isNull();
    }

    @Test
    public void shouldNotLoadAnyCqlFilesInSubDirectories() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_sub_directories"));

        //when
        try {
            MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);
        } catch (RuntimeException e) {
            // nada
        }

        //then
        TableMetadata tableMetadata = CLUSTER.getMetadata().getKeyspace(TEST_KEYSPACE).getTable("status");
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
        Session session = CLUSTER.connect(TEST_KEYSPACE);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE, cqlPaths);

        //when
        MIGRATOR.clean(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE);

        //then
        assertThat(CLUSTER.getMetadata().getKeyspace(TEST_KEYSPACE)).as("keyspace should be gone").isNull();
    }

    @Test
    public void shouldFailSilentlyIfCleaningANonExistingKeyspace() throws Exception {
        MIGRATOR.clean(CASSANDRA_HOSTS, BINARY_PORT, TEST_KEYSPACE);
    }

}
