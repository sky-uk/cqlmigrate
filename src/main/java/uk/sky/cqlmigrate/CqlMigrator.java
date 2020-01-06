package uk.sky.cqlmigrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import uk.sky.cqlmigrate.exception.CannotAcquireLockException;
import uk.sky.cqlmigrate.exception.CannotReleaseLockException;
import uk.sky.cqlmigrate.exception.ClusterUnhealthyException;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;

/**
 * Interface for managing application schema changes in a synchronized fashion.
 * The implementation classes are expected to implement some locking mechanism so that there are no concurrent schema modifications when multiple threads attempt to make the schema changes on the
 * same cassandra keyspace.
 */

public interface CqlMigrator {
    /**
     * If all nodes are up and a lock can be acquired this runs migration
     * starting with bootstrap.cql and then the rest in alphabetical order.
     * If the migration fails for any reason, the lock will not be released
     * and manual intervention is required to cleanup and fix the issue.
     *
     * @param hosts       Comma separated list of cassandra hosts
     * @param port        Native transport port for the above cassandra nodes
     * @param username    Username for Cassandra's internal authentication using PasswordAuthenticator
     *                    (if using AllowAllAuthenticator, can be set to any value)
     * @param password    Password for Cassandra's internal authentication using PasswordAuthenticator
     *                    (if using AllowAllAuthenticator, can be set to any value)
     * @param keyspace    Keyspace name for which the schema migration needs to be applied
     * @param directories Comma separated list of directory paths containing the cql statements for the schema change
     * @throws ClusterUnhealthyException                           if any nodes are down or the schema is not in agreement before running migration
     * @throws CannotAcquireLockException                          if any of the queries to acquire lock fail or
     *                                                             {@link CassandraLockConfig.CassandraLockConfigBuilder#withTimeout(Duration)}
     *                                                             is reached before lock can be acquired.
     * @throws CannotReleaseLockException                          if any of the queries to release lock fail
     * @throws IllegalArgumentException                            if any file types other than .cql are found
     * @throws IllegalStateException                               if cql file has changed after migration has been run
     * @throws com.datastax.oss.driver.api.core.DriverException if any of the migration queries fails
     */
    void migrate(String[] hosts, int port, String username, String password, String keyspace, Collection<Path> directories);

    /**
     * If all nodes are up and a lock can be acquired this runs migration
     * starting with bootstrap.cql and then the rest in alphabetical order.
     * If the migration fails for any reason, the lock will not be released
     * and manual intervention is required to cleanup and fix the issue.
     *
     * @param session     Session to a cassandra cluster
     * @param keyspace    Keyspace name for which the schema migration needs to be applied
     * @param directories Comma separated list of directory paths containing the cql statements for the schema change
     * @throws ClusterUnhealthyException                           if any nodes are down or the schema is not
     *                                                             in agreement before running migration
     * @throws CannotAcquireLockException                          if any of the queries to acquire lock fail or
     *                                                             {@link CassandraLockConfig.CassandraLockConfigBuilder#withTimeout(Duration)}
     *                                                             is reached before lock can be acquired.
     * @throws CannotReleaseLockException                          if any of the queries to release lock fail
     * @throws IllegalArgumentException                            if any file types other than .cql are found
     * @throws IllegalStateException                               if cql file has changed after migration has been run
     * @throws com.datastax.oss.driver.api.core.DriverException if any of the migration queries fails
     */
    void migrate(CqlSession session, String keyspace, Collection<Path> directories);

    /**
     * Drops keyspace if it exists
     *
     * @param hosts    Comma separated list of cassandra hosts
     * @param port     Native transport port for the above cassandra nodes
     * @param username Username for Cassandra's internal authentication using PasswordAuthenticator
     *                 (if using AllowAllAuthenticator, can be set to any value)
     * @param password Password for Cassandra's internal authentication using PasswordAuthenticator
     *                 (if using AllowAllAuthenticator, can be set to any value)
     * @param keyspace Keyspace name for which the schema migration needs to be applied
     * @throws com.datastax.oss.driver.api.core.DriverException if query fails
     */
    void clean(String[] hosts, int port, String username, String password, String keyspace);

    /**
     * Drops keyspace if it exists
     *
     * @param session  Session to a cassandra cluster
     * @param keyspace Keyspace name for which the schema migration needs to be applied
     * @throws com.datastax.oss.driver.api.core.DriverException if query fails
     */
    void clean(Session session, String keyspace);
}