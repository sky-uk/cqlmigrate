package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class CassandraLockConfigTest {

    @Test
    public void shouldBuildCassandraLockConfigWithDefaultConsistencyLevelLocalOne() throws Throwable {

        CassandraLockConfig lockConfig = CassandraLockConfig.builder()
                .build();

        Assertions.assertThat(lockConfig.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    }

    @Test
    public void shouldBuildCassandraLockConfigWithConsistencyLevelOfAllWhenSetInBuilder(){
        CassandraLockConfig lockConfig = CassandraLockConfig.builder()
                .withConsistencyLevel(ConsistencyLevel.ALL)
                .build();

        Assertions.assertThat(lockConfig.getConsistencyLevel()).isEqualTo(ConsistencyLevel.ALL);
    }

}
