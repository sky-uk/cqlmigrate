package uk.sky.cqlmigrate;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class CqlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlLoader.class);

    private CqlLoader() {}

    static void load(SessionContext sessionContext, List<String> cqlStatements) {
        try {
            cqlStatements.stream()
                    .map(stringStatement -> new SimpleStatement(stringStatement).setConsistencyLevel(sessionContext.getWriteConsistencyLevel()))
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
