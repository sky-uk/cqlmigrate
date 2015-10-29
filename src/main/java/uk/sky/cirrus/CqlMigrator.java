package uk.sky.cirrus;

import com.datastax.driver.core.Session;

import java.nio.file.Path;
import java.util.Collection;

public interface CqlMigrator {
    void migrate(String[] hosts, int port, String keyspace, Collection<Path> directories);
    void migrate(Session session, String keyspace, Collection<Path> directories);
    void clean(String[] hosts, int port, String keyspace);
    void clean(Session session, String keyspace);
}