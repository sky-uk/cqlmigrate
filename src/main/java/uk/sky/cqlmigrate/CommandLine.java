package uk.sky.cqlmigrate;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Preconditions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class CommandLine {

    /**
     * Allows cql migrate to be run on the command line.
     * Set hosts, keyspace and directories using system properties.
     */
    public static void main(String[] args) {
        String hosts = System.getProperty("hosts");
        String keyspace = System.getProperty("keyspace");
        String directoriesProperty = System.getProperty("directories");
        String port = System.getProperty("port");

        Preconditions.checkNotNull(hosts, "'hosts' property should be provided having value of a comma separated list of cassandra hosts");
        Preconditions.checkNotNull(keyspace, "'keyspace' property should be provided having value of the cassandra keyspace");
        Preconditions.checkNotNull(directoriesProperty, "'directories' property should be provided having value of the comma separated list of paths to cql files");

        Collection<Path> directories = Arrays.stream(directoriesProperty.split(","))
                .map(Paths::get)
                .collect(Collectors.toList());

        CqlMigratorFactory.create(
                CqlMigratorConfig.builder()
                        .cassandraLockConfig(CassandraLockConfig.builder().build())
                        .readConsistencyLevel(ConsistencyLevel.ALL)
                        .writeConsistencyLevel(ConsistencyLevel.ALL)
                        .build()
        ).migrate(hosts.split(","), port == null ? 9042 : Integer.parseInt(port), keyspace, directories);
    }
}
