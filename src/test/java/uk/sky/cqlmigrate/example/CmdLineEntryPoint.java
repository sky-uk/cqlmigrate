package uk.sky.cqlmigrate.example;

public class CmdLineEntryPoint {

    public static void main(String[] args) throws Exception {

        try {
            CqlMigrateInvoker cqlMigrateInvoker = new CqlMigrateInvoker();

            // @Before
            CqlMigrateInvoker.setupCassandra();
            cqlMigrateInvoker.setUp();

            // Do the test
            cqlMigrateInvoker.doMigrate();

            // @After
            cqlMigrateInvoker.tearDown();
            CqlMigrateInvoker.tearDownCassandra();

            System.exit(0);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
