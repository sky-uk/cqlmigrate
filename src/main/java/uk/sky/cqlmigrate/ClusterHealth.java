package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;

import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

class ClusterHealth {

    private final Cluster cluster;

    ClusterHealth(Cluster cluster) {
        this.cluster = cluster;
    }

    void check() throws ClusterUnhealthyException {

        List<InetAddress> unhealthyHosts = cluster.getMetadata().getAllHosts()
                .stream()
                .filter(host -> !host.isUp())
                .map(Host::getAddress)
                .collect(Collectors.toList());

        if (!unhealthyHosts.isEmpty()) {
            throw new ClusterUnhealthyException("Cluster not healthy, the following hosts are down: " + unhealthyHosts);
        }
    }
}
