package uk.sky.cqlmigrate;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class ClusterHealth {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealth.class);

    private final Cluster cluster;

    ClusterHealth(Cluster cluster) {
        this.cluster = cluster;
    }

    void check() throws ClusterUnhealthyException {

        log.debug("Checking cluster health");

        Set<Host> allHosts = cluster.getMetadata().getAllHosts();

        List<InetAddress> unhealthyHosts = allHosts
                .stream()
                .filter(host -> !host.isUp())
                .map(Host::getAddress)
                .collect(Collectors.toList());

        if (!unhealthyHosts.isEmpty()) {
            throw new ClusterUnhealthyException("Cluster not healthy, the following hosts are down: " + unhealthyHosts);
        }

        log.debug("All hosts healthy: {}", allHosts);
    }
}
