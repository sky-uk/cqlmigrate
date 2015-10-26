package uk.sky.cirrus;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import uk.sky.cirrus.exception.ClusterUnhealthyException;

public class ClusterHealth {

    private final Cluster cluster;

    public ClusterHealth(Cluster cluster) {
        this.cluster = cluster;
    }

    public void check() {
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
