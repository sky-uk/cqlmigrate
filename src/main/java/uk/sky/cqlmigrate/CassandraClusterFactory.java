package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;

public class CassandraClusterFactory {

    /**
     * Creates an instance of cassandra {@link Cluster} based on the provided configuration
     *
     * @return a configured Cluster
     */
    public static Cluster createCluster(String[] hosts, int port, String username, String password) {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(hosts)
                .withPort(port);

        if (username != null && password != null) {
            builder = builder.withCredentials(username, password);
        }

        return builder.build();
    }
}
