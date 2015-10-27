package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import uk.sky.cirrus.exception.ClusterUnhealthyException;

class ClusterHealth {

    private final Cluster cluster;

    ClusterHealth(Cluster cluster) {
        this.cluster = cluster;
    }

    void check() throws ClusterUnhealthyException{
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (!host.isUp()) {
                throw new ClusterUnhealthyException("Cluster not healthy, at least 1 host is down: " + host.getAddress());
            }
        }

        if (!cluster.getMetadata().checkSchemaAgreement()) {
            throw new ClusterUnhealthyException("Cluster not healthy, schema not in agreement");
        }
    }
}
