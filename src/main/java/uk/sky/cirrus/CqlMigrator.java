package uk.sky.cirrus;

import java.nio.file.Path;
import java.util.Collection;

public interface CqlMigrator {
    void migrate(String[] hosts, int port, String keyspace, Collection<Path> directories);
    void clean(String[] hosts, int port, String keyspace);
}