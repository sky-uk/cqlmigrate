package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
class CqlLoader {
    private CqlLoader() {
    }

    static void load(Session session, List<String> cqlStatements, ConsistencyLevel writeConsistency) {
        try {
            cqlStatements.stream()
                    .map(stringStatement -> new SimpleStatement(stringStatement).setConsistencyLevel(writeConsistency))
                    .forEach(statement -> {
                        log.debug("Executing cql statement {}", statement);
                        session.execute(statement);
                    });
        } catch (DriverException e) {
            log.error("Failed to execute cql statements {}: {}", cqlStatements, e.getMessage());
            throw e;
        }
    }
}
