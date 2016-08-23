package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class CqlMigratorConfigTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @DataPoint
    public static ConsistencyLevel CONSISTENCY_LEVEL_BAD = null;

    @DataPoint
    public static ConsistencyLevel CONSISTENCY_LEVEL_GOOD = ConsistencyLevel.ALL;

    @DataPoint
    public static CassandraLockConfig CASSANDRA_LOCK_CONFIG_GOOD = CassandraLockConfig.builder().build();

    @DataPoint
    public static CassandraLockConfig CASSANDRA_LOCK_CONFIG_BAD = null;

    @Theory
    public void shouldThrowNullPointerExceptionWhenAnyParameterPassedIntoBuilderIsNull(CassandraLockConfig config, ConsistencyLevel readCL, ConsistencyLevel writeCL) {
        Assume.assumeTrue(config == null || readCL == null || writeCL == null);

        expectedException.expect(NullPointerException.class);
        CqlMigratorConfig.builder().withReadConsistencyLevel(readCL).withWriteConsistencyLevel(writeCL).withCassandraLockConfig(config).build();
    }
}
