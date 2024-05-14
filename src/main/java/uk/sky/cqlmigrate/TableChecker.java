package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;

public class TableChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableChecker.class);
    public static final String AWSK_CLUSTER_NAME = "Amazon Keyspaces";
    public static final String TABLE_ACTIVE_STATUS = "ACTIVE";


    public static void check(CqlSession session, String keyspace) {
        String tableUpdateTimeoutString = System.getProperty("tableUpdateTimeout");
        Duration tableUpdateTimeout = Objects.nonNull(tableUpdateTimeoutString) ? Duration.parse(tableUpdateTimeoutString) : null;
        if (nonNull(tableUpdateTimeout) && isAWSK(session)) {
            LOGGER.info("Waiting for tables to be provisioned");
            Awaitility.await()
                    .pollDelay(0, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .atMost(tableUpdateTimeout)
                    .until(() -> {
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
                    });
            LOGGER.info("All tables are ready");
        }
    }

    private static boolean isAWSK(CqlSession session) {
        ResultSet result = session.execute("SELECT cluster_name FROM system.local WHERE key='local';");
        return AWSK_CLUSTER_NAME.equals(result.one().getString("cluster_name"));
    }
}
