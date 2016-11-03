package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;

public class CassandraClusterFactory {

    /**
     * Creates an instance of cassandra {@link Cluster} based on the provided configuration
     *
     * @param hosts Addresses of the nodes to add as contact points (as described in
     *              {@link Cluster.Builder#addContactPoint}).
     * @param port The port to use to connect to the Cassandra hosts.
     * @param username the username to use to login to Cassandra hosts.
     * @param password the password corresponding to {@code username}.
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
