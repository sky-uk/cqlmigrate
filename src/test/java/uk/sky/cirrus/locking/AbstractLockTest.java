package uk.sky.cirrus.locking;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.types.ColumnMetadata;
import org.scassandra.junit.ScassandraServerRule;
import uk.sky.cirrus.util.PortScavenger;

import java.util.UUID;

import static org.scassandra.http.client.PrimingRequest.then;

public abstract class AbstractLockTest {

    protected static final String REPLICATION_CLASS = "SimpleStrategy";
    protected static final int REPLICATION_FACTOR = 1;
    protected static final LockConfig DEFAULT_LOCK_CONFIG = LockConfig.builder().build();
    protected static final String LOCK_KEYSPACE = "lock-keyspace";

    protected Session session;
    protected Cluster cluster;
    protected PrimingClient primingClient;
    protected ActivityClient activityClient;

    public abstract int getBinaryPort();

    abstract ScassandraServerRule getScassandra();

    @Before
    public void baseSetup() throws Exception {
        primingClient = getScassandra().primingClient();
        activityClient = getScassandra().activityClient();
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(getBinaryPort())
                .build();
        session = cluster.connect();

        primingClient.prime(PrimingRequest.queryBuilder()
                        .withQuery("INSERT INTO locks.locks (name, client) VALUES (?, ?) IF NOT EXISTS")
                        .withThen(then()
                                .withColumnTypes(ColumnMetadata.column("client", PrimitiveType.UUID), ColumnMetadata.column("[applied]", PrimitiveType.BOOLEAN))
                                .withRows(ImmutableMap.of("client", UUID.randomUUID(), "[applied]", true)))
                        .build()
        );
    }

    @After
    public void baseTearDown() throws Exception {
        cluster.close();
    }
}
