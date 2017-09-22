package uk.sky.cqlmigrate;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;

public class CqlMigratorImplTest {

    private static final String[] CASSANDRA_HOSTS = {"localhost"};
    private static String username = "cassandra";
    private static String password = "cassandra";
    private static int binaryPort;
    private static Cluster cluster;
    private static Session session;
    private static final String TEST_KEYSPACE = "cqlmigrate_test";
    private static final String LOCK_NAME = TEST_KEYSPACE + ".schema_migration";

    private static final CqlMigratorImpl MIGRATOR = new CqlMigratorImpl(CqlMigratorConfig.builder()
            .withLockConfig(CassandraLockConfig.builder().build())
            .withReadConsistencyLevel(ConsistencyLevel.ALL)
            .withWriteConsistencyLevel(ConsistencyLevel.ALL)
            .build(), new SessionContextFactory());

    private ExecutorService executorService;

    @BeforeClass
    public static void setupCassandra() throws ConfigurationException, IOException, TTransportException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, 30000);
        binaryPort = EmbeddedCassandraServerHelper.getNativeTransportPort();

        cluster = Cluster.builder().addContactPoints(CASSANDRA_HOSTS).withPort(binaryPort).withCredentials(username, password).build();
        session = cluster.connect();
    }

    @Before
    public void setUp() throws Exception {
        session.execute("DROP KEYSPACE IF EXISTS cqlmigrate_test");
        session.execute("CREATE KEYSPACE IF NOT EXISTS cqlmigrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS cqlmigrate.locks (name text PRIMARY KEY, client text)");
        executorService = Executors.newFixedThreadPool(1);
    }

    @After
    public void tearDown() {
        session.execute("TRUNCATE cqlmigrate.locks");
        executorService.shutdownNow();
        System.clearProperty("hosts");
        System.clearProperty("keyspace");
        System.clearProperty("directories");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        session.execute("DROP KEYSPACE cqlmigrate");
        cluster.close();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    }

    private Path getResourcePath(String resourcePath) throws URISyntaxException {
        return Paths.get(ClassLoader.getSystemResource(resourcePath).toURI());
    }

    @Test
    public void shouldRunTheBootstrapCqlIfKeyspaceDoesNotExist() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        try {
            cluster.connect(TEST_KEYSPACE);
        } catch (Throwable t) {
            fail("Should have successfully connected, but got " + t);
        }
    }

    @Test(timeout = 5000)
    public void shouldThrowCannotAcquireLockExceptionIfLockCannotBeAcquiredAfterTimeout() throws Exception {
        //given
        CqlMigrator migrator = new CqlMigratorImpl(CqlMigratorConfig.builder()
                .withLockConfig(CassandraLockConfig.builder().withPollingInterval(ofMillis(50)).withTimeout(ofMillis(300)).build())
                .withReadConsistencyLevel(ConsistencyLevel.ALL)
                .withWriteConsistencyLevel(ConsistencyLevel.ALL)
                .build(), new SessionContextFactory());

        String client = UUID.randomUUID().toString();
        session.execute("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?)", LOCK_NAME, client);

        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        Future<?> future = executorService.submit(() -> migrator.migrate(
                CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths));

        Thread.sleep(310);
        Throwable throwable = catchThrowable(future::get);

        //then
        assertThat(throwable).isInstanceOf(ExecutionException.class);
        assertThat(throwable.getCause()).isInstanceOf(CannotAcquireLockException.class);
        assertThat(throwable.getCause().getMessage()).isEqualTo("Lock currently in use");
    }

    @Test
    public void shouldMigrateSchemaIfLockCanBeAcquired() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

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
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        ResultSet resultSet = session.execute("SELECT * FROM cqlmigrate.locks WHERE name = ?", LOCK_NAME);
        assertThat(resultSet.isExhausted()).as("Is lock released").isTrue();
    }

    @Test
    public void shouldNotRemoveLockAfterMigrationFailed() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap_missing_semicolon"));

        //when
        try {
            MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        } catch (RuntimeException ignore) {
        }

        //then
        ResultSet resultSet = session.execute("SELECT * FROM cqlmigrate.locks WHERE name = ?", LOCK_NAME);
        assertThat(resultSet.isExhausted()).as("Is lock released").isFalse();
    }

    @Test
    public void shouldRemoveLockAfterMigrationFailedIfUnlockOnFailureIsSetToTrue() throws Exception {
        //given
        CqlMigrator migrator = new CqlMigratorImpl(CqlMigratorConfig.builder()
            .withLockConfig(CassandraLockConfig.builder().unlockOnFailure().build())
            .withReadConsistencyLevel(ConsistencyLevel.ALL)
            .withWriteConsistencyLevel(ConsistencyLevel.ALL)
            .build(), new SessionContextFactory());
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap_missing_semicolon"));

        //when
        try {
            migrator.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
        } catch (RuntimeException ignore) {
        }

        //then
        ResultSet resultSet = session.execute("SELECT * FROM cqlmigrate.locks WHERE name = ?", LOCK_NAME);
        assertThat(resultSet.isExhausted()).as("Is lock released").isTrue();
    }

    @Test
    public void shouldRetryWhenAcquiringLockIfNotInitiallyAvailable() throws Exception {
        //given
        session.execute("INSERT INTO cqlmigrate.locks (name, client) VALUES (?, ?)", LOCK_NAME, UUID.randomUUID().toString());
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap"));

        //when
        Future<?> future = executorService.submit(() -> MIGRATOR.migrate(
                CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths));
        session.execute("TRUNCATE cqlmigrate.locks");
        Thread.sleep(1000);
        future.get();

        //then
        try {
            cluster.connect(TEST_KEYSPACE);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForBootstrapWithMissingSemiColon() throws Exception {
        //given
        Collection<Path> cqlPaths = singletonList(getResourcePath("cql_bootstrap_missing_semicolon"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionWhenMultipleBootstrap() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_bootstrap"), getResourcePath("cql_bootstrap_duplicate"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        cluster.connect(TEST_KEYSPACE);
    }

    @Test
    public void shouldLoadCqlFilesInOrderAcrossDirectories() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

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
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //when
        cqlPaths.add(getResourcePath("cql_valid_three"));
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

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
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //then
        ResultSet rs = session.execute("select * from schema_updates");
        for (Row row : rs) {
            assertThat(row.getTimestamp("applied_on")).as("applied_on").isNotNull().isAfter(now);
        }
    }

    @Test
    public void schemaUpdatesTableByPassingCassandraSession() throws Exception {
        //given
        Date now = new Date();
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));

        //when
        MIGRATOR.migrate(session, TEST_KEYSPACE, cqlPaths);

        //then
        ResultSet rs = session.execute("select * from schema_updates");
        for (Row row : rs) {
            assertThat(row.getTimestamp("applied_on")).as("applied_on").isNotNull().isAfter(now);
        }
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailIfThereAreDuplicateCqlFilenames() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"),
                getResourcePath("cql_valid_duplicate_filename"));

        //when
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
    }

    @Test
    public void shouldNotLoadAnyOfTheCqlFilesIfThereAreDuplicateCqlFilenames() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"),
                getResourcePath("cql_valid_duplicate_filename"));

        //when
        try {
            MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
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
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //when
        try {
            Collection<Path> differentContentsPaths = singletonList(getResourcePath("cql_create_status_different_contents"));
            MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, differentContentsPaths);
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
            MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);
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
        System.setProperty("port", String.valueOf(binaryPort));

        //when
        CqlMigratorImpl.main(new String[]{});

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from status where dependency = 'developers'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("waste_of_space")).isEqualTo("true");
    }

    @Test
    public void shouldThrowExceptionWithAppropriateMessageIfHostsNotSetForMain() throws Exception {
        //given
        System.setProperty("keyspace", TEST_KEYSPACE);
        System.setProperty("directories", getResourcePath("cql_valid_one").toString() + "," + getResourcePath("cql_valid_two").toString());

        //when
        Throwable throwable = catchThrowable(() -> CqlMigratorImpl.main(new String[]{}));
        assertThat(throwable).isNotNull().isInstanceOf(NullPointerException.class);
        assertThat(throwable.getMessage()).isEqualTo("'hosts' property should be provided having value of a comma separated list of cassandra hosts");
    }

    @Test
    public void shouldThrowExceptionWithAppropriateMessageIfKeyspaceNotSetForMain() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("directories", getResourcePath("cql_valid_one").toString() + "," + getResourcePath("cql_valid_two").toString());

        //when
        Throwable throwable = catchThrowable(() -> CqlMigratorImpl.main(new String[]{}));
        assertThat(throwable).isNotNull().isInstanceOf(NullPointerException.class);
        assertThat(throwable.getMessage()).isEqualTo("'keyspace' property should be provided having value of the cassandra keyspace");
    }

    @Test
    public void shouldThrowExceptionWithAppropriateMessageIfDirectoriesNotSetForMain() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("keyspace", TEST_KEYSPACE);

        //when
        Throwable throwable = catchThrowable(() -> CqlMigratorImpl.main(new String[]{}));
        assertThat(throwable).isNotNull().isInstanceOf(NullPointerException.class);
        assertThat(throwable.getMessage()).isEqualTo("'directories' property should be provided having value of the comma separated list of paths to cql files");
    }

    @Test
    public void shouldRemoveAllDataWhenCleaningAKeyspace() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));
        MIGRATOR.migrate(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE, cqlPaths);

        //when
        MIGRATOR.clean(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE);

        //then
        assertThat(cluster.getMetadata().getKeyspace(TEST_KEYSPACE)).as("keyspace should be gone").isNull();
    }

    @Test
    public void shouldCleanAKeyspaceOnACassandraSession() throws Exception {
        //given
        Collection<Path> cqlPaths = asList(getResourcePath("cql_valid_one"), getResourcePath("cql_valid_two"));
        MIGRATOR.migrate(session, TEST_KEYSPACE, cqlPaths);

        //when
        MIGRATOR.clean(session, TEST_KEYSPACE);

        //then
        assertThat(cluster.getMetadata().getKeyspace(TEST_KEYSPACE)).as("keyspace should be gone").isNull();
    }

    @Test
    public void shouldFailSilentlyIfCleaningANonExistingKeyspace() throws Exception {
        MIGRATOR.clean(CASSANDRA_HOSTS, binaryPort, username, password, TEST_KEYSPACE);
    }

    @Test
    public void shouldCopeWithEscapedSingleQuotes() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("keyspace", TEST_KEYSPACE);
        System.setProperty("directories", getResourcePath("cql_rolegraphs_one").toString() + "," + getResourcePath("cql_rolegraphs_two").toString());
        System.setProperty("port", String.valueOf(binaryPort));

        //when
        CqlMigratorImpl.main(new String[]{});

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from role_graphs where provider = 'SKY'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString("graphml")).isEqualTo("some text with something 'quoted', some more text");
    }

    @Test
    public void shouldCopeWithLongAndComplexCqlValues() throws Exception {
        //given
        System.setProperty("hosts", "localhost");
        System.setProperty("keyspace", TEST_KEYSPACE);
        System.setProperty("directories", getResourcePath("cql_rolegraphs_one").toString() + "," + getResourcePath("cql_rolegraphs_three").toString());
        System.setProperty("port", String.valueOf(binaryPort));

        //when
        CqlMigratorImpl.main(new String[]{});

        //then
        Session session = cluster.connect(TEST_KEYSPACE);
        ResultSet rs = session.execute("select * from role_graphs where provider = 'SKY'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
    }
}
