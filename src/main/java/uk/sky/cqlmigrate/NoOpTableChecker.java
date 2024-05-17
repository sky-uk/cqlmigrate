package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;

class NoOpTableChecker implements TableChecker {

    @Override
    public void check(CqlSession session, String keyspace) {
    }
}
