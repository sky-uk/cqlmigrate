package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import uk.sky.cirrus.exception.ClusterUnhealthyException;

import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

class ClusterHealth {

    private final Cluster cluster;

    ClusterHealth(Cluster cluster) {
        this.cluster = cluster;
    }

    void check() throws ClusterUnhealthyException{

        final List<InetAddress> unhealthyHosts = cluster.getMetadata().getAllHosts()
                .parallelStream()
                .filter(host -> !host.isUp())
                .map(Host::getAddress)
                .collect(Collectors.toList());

        if(!unhealthyHosts.isEmpty()){
            throw new ClusterUnhealthyException("Cluster not healthy, the following hosts are down: " + unhealthyHosts);
        }

        if (!cluster.getMetadata().checkSchemaAgreement()) {
            throw new ClusterUnhealthyException("Cluster not healthy, schema not in agreement");
        }
    }
}
