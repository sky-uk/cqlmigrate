package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Builder
@Data
public class CqlMigratorConfig {
    @NonNull
    private final CassandraLockConfig cassandraLockConfig;
    @NonNull
    private final ConsistencyLevel readConsistencyLevel;
    @NonNull
    private final ConsistencyLevel writeConsistencyLevel;
}
