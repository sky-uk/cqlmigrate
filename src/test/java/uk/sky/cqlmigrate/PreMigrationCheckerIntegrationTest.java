package uk.sky.cqlmigrate;


import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PreMigrationCheckerIntegrationTest {

    private PreMigrationChecker preMigrationChecker;

    @Before
    public void setup() {
        preMigrationChecker = new PreMigrationChecker();
    }

    @Test
    public void migrationAlwaysNeeded() {
        assertThat(preMigrationChecker.migrationIsNeeded()).isTrue();
    }

}