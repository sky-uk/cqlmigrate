package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

class ClusterHealth {

    private static final Logger log = LoggerFactory.getLogger(ClusterHealth.class);

    private final Session cluster;

    ClusterHealth(Session cluster) {
        this.cluster = cluster;
    }

    void check() throws ClusterUnhealthyException {

        log.debug("Checking cluster health");

        Map<UUID, Node> nodes = cluster.getMetadata().getNodes();

        List<InetAddress> unhealthyHosts = nodes.entrySet().stream()
            .filter(host -> host.getValue().getState().equals(NodeState.DOWN))
            .map(host -> host.getValue().getBroadcastAddress().get().getAddress())
            .collect(Collectors.toList());

        if (!unhealthyHosts.isEmpty()) {
            throw new ClusterUnhealthyException("Cluster not healthy, the following hosts are down: " + unhealthyHosts);
        }

        log.debug("All hosts healthy: {}", nodes.entrySet());
    }
}
