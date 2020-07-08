package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.oss.driver.api.core.type.reflect.GenericType.BOOLEAN;

class CqlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlLoader.class);

    private CqlLoader() {
    }

    static void load(SessionContext sessionContext, List<String> cqlStatements) {
        if (!cqlStatements.isEmpty()) {
            sessionContext.checkClusterHealth();
        }
        try {
            cqlStatements.stream()
                .map(stringStatement -> SimpleStatement.newInstance(stringStatement).setConsistencyLevel(sessionContext.getWriteConsistencyLevel()))
                .forEach(statement -> {
                    LOGGER.debug("Executing cql statement {}", statement);
                    sessionContext.getSession().execute(statement);
                });
        } catch (DriverException e) {
            LOGGER.error("Failed to execute cql statements {}: {}", cqlStatements, e.getMessage());
            throw e;
        }
    }
}
