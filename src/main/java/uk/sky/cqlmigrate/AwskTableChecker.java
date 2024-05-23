package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

class AwskTableChecker implements TableChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(AwskTableChecker.class);
    public static final String TABLE_ACTIVE_STATUS = "ACTIVE";

    private final Duration timeout;
    private final Duration initDelay;

    AwskTableChecker(Duration initDelay, Duration timeout) {
        this.timeout = timeout;
        this.initDelay = initDelay;
    }

    @Override
    public void check(CqlSession session, String keyspace) {
        LOGGER.info("Waiting for tables to be provisioned");
        Awaitility.await()
                .pollDelay(initDelay)
                .pollInterval(5, TimeUnit.SECONDS)
                .atMost(timeout)
                .until(() -> allTablesActive(session, keyspace));
        LOGGER.info("All tables are ready");
    }

    private Boolean allTablesActive(CqlSession session, String keyspace) {
        try {
            ResultSet result = session.execute("SELECT table_name, status FROM system_schema_mcs.tables WHERE keyspace_name=?;", keyspace);
            return !result.all().stream()
                    .filter(row -> !TABLE_ACTIVE_STATUS.equals(row.getString("status")))
                    .peek(row -> LOGGER.info("Table {} is still in {} state", row.getString("table_name"), row.getString("status")))
                    .findAny()
                    .isPresent();
        } catch (Exception ex) {
            return false;
        }
    }
}
