# Cassandra CQL migration tool

cqlmigrate is a library for performing schema migrations on a cassandra cluster.

It is best used as a dependency in your application, but can also be used
standalone.

## Prerequisites

The locks keyspace and table needs to be created before running the migration.

    CREATE KEYSPACE IF NOT EXISTS cqlmigrate_locks WITH replication = {'class': 'REPLICATION_CLASS', 'replication_factor': REPLICATION_FACTOR };
    CREATE TABLE IF NOT EXISTS cqlmigrate_locks.locks (name text PRIMARY KEY, client text);

## Library usage

The API is contained in `CqlMigrator`, and works on directories of files ending with `.cql`.

For example, to apply all `.cql` files located in `/cql` in the classpath:

Configure the lock:

    CassandraLockConfig lockConfig = CassandraLockConfig.builder()
                    .withTimeout(Duration.standardSeconds(3))
                    .withPollingInterval(Duration.millis(500))
                    .withClientId("127.0.0.1")
                    .build();
                    
Or

    CassandraLockConfig lockConfig = CassandraLockConfig.builder()
                    .withTimeout(Duration.standardSeconds(3))
                    .withPollingInterval(Duration.millis(500))
                    .withClientId("127.0.0.1")
                    .build();

Then:                    

    CqlMigrator migrator = new CqlMigrator(lockConfig);
    Path schemas = Paths.get(ClassLoader.getSystemResource("/cql").toURI());
    migrator.migrate(asList("localhost"), "my_keyspace", asList(schemas));

Examples of cql files can be seen in `src/test/resources`.

## Standalone usage 

```sh
$ java -Dhosts=localhost,192.168.1.1 -Dkeyspace=my_keyspace -Ddirectories=cql-common,cql-local -jar cqlmigrate.jar
```

## What it does

1. Checks all nodes are up and their schema's are in agreement.

2. Tries to acquire a lock for the keyspace you are migrating.
   If it can't initially be acquired it will continue to retry at a set polling time until the timeout is reached.  

3. Looks for a `bootstrap.cql` file and runs it first. This file should contain your keyspace definition.

   For example:
   ```
   CREATE KEYSPACE my_keyspace
     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
   ```        

4. Applies `.cql` files one by one, sorted by filename in ascending order. It is suggested you prefix
   your files with a datetime to order them.

   For example:
   ```
   /cql/2015-05-02-19:20-create-airplanes-table.cql
   /cql/2015-05-03-14:19-add-manufacturer-column.cql
   ```

   Any previously applied files will be skipped.
   
5. Releases the lock.

### schema_updates table

This table is used to determine what has been previously applied.

    SELECT * FROM schema_updates;

     filename                         | applied_on               | checksum
    ----------------------------------+--------------------------+------------------------------------------
               0001-create-tables.cql | 2015-04-08 12:10:04+0100 | ec19dfac7ede62b2a40c0f39706b237cd5c30da6
                     0002-dataset.cql | 2015-04-08 12:10:04+0100 | 4fa2d6c4fae9950f0c9140ae2eb57fe689192b4a
                0003-initial-date.cql | 2015-04-08 12:10:04+0100 | 19d0c9522b6464a06b18192c6e04233f83e78a84

    (3 rows)

It also maintains a checksum to ensure the script hasn't changed since it was last applied.

### locks keyspace and table

The locks keyspace replication class and factor can be configured using the LocksConfig.
This table is used to keep track of what locks are currently in place.

    SELECT * FROM locks;
    
     name                                | client
    -------------------------------------+--------------------------------------
     airplanes_keyspace.schema_migration | 2a4ec2ae-d3d1-4b33-86a9-eb844e35eeeb
    
    (1 rows)

Each lock will be deleted by Cql Migrate once the migration is complete.

## Supported Cassandra versions

This project has been tested against the following versions:
* 2.1.7
* DSE 4.7.3 (2.1.8)
* 2.2.2
* 2.2.3

## Caveats

Cassandra is an eventually consistent, AP database, and so applying schema updates are not as simple
as a strongly consistent database.

* Certain schema changes can cause data corruption on cassandra. Be very careful changing a schema for a
  table that is being actively written to. Generally, adding columns is safe (but be careful with
  collection types). Test your migration before rolling out to production.

* AP properties of Cassandra also apply to schema updates - so it is possible for your cluster to have an
  inconsistent schema across nodes in case of split brain or other situation. We use `ConsistencyLevel.ALL`
  to try and alleviate this.
  
# Todo

* Split up into modules
  * core - contains the library only
  * cli - contains a standalone jar that can be run at the command line
  * dw - dropwizard plugin to add cqlMigrate task
  * graphml - plugin to add support for `.graphml` files 
* Add axion-release plugin to handle release numbers
* Add to jcenter for public consumption
* Add to travisci

# Contributors

Originally developed by the Cirrus team at Sky.

- Adam Dougal
- James Booth
- James Ravn
- Adrian Ng
- Malinda Rajapakse
- Ashutosh Gawande
- Dominic Mullings
- Yoseph Samuel
- David Sale
- Supreeth Rao
