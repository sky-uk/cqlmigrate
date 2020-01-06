package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CassandraClusterFactory {

    // test

    /**
     * Creates an instance of cassandra {@link Session} based on the provided configuration
     *
     * @param hosts Addresses of the nodes to add as contact points (as described in
     *              {@link CqlSession}).
     * @param port The port to use to connect to the Cassandra hosts.
     * @param username the username to use to login to Cassandra hosts.
     * @param password the password corresponding to {@code username}.
     * @return a configured Session
     */
    public static Session createCluster(String[] hosts, int port, String username, String password) {

        List<InetSocketAddress> cassandraHosts = Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());

        if (username != null && password != null) {
            return CqlSession.builder()
                .addContactPoints(cassandraHosts)
                .withAuthCredentials(username, password).build();
        }

        return CqlSession.builder().addContactPoints(cassandraHosts).build();
    }
}
