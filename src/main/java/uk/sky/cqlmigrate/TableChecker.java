package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;

interface TableChecker {
    void check(CqlSession session, String keyspace);
}
