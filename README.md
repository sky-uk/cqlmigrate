# Cassandra CQL migration tool

cqlmigrate is a library for performing schema migrations on a cassandra cluster.

It is best used as an application dependency, but can also be used standalone.

## Adding as a Maven dependency

```xml
<dependency>
  <groupId>uk.sky</groupId>
  <artifactId>cqlmigrate</artifactId>
  <version>0.9.1</version>
  <type>pom</type>
</dependency>
```

## Adding as a Gradle dependency

```groovy
compile 'uk.sky:cqlmigrate:0.9.1'
```

## Cassandra Prerequisites

The locks keyspace and table needs to be created before running any migrations.

    CREATE KEYSPACE IF NOT EXISTS cqlmigrate WITH replication = {'class': 'REPLICATION_CLASS', 'replication_factor': REPLICATION_FACTOR };
    CREATE TABLE IF NOT EXISTS cqlmigrate.locks (name text PRIMARY KEY, client text);

## Library usage

To apply all `.cql` files located in `/cql` in the classpath:

```java
// Configure locking for coordination of multiple nodes
CassandraLockConfig lockConfig = CassandraLockConfig.builder()
        .withTimeout(Duration.standardSeconds(3))
        .withPollingInterval(Duration.millis(500))
        .withClientId("node-uuid")
        .build())

// Create a migrator and run it
CqlMigrator migrator = CqlMigratorFactory.create(lockConfig)
Path schemas = Paths.get(ClassLoader.getSystemResource("/cql").toURI());
migrator.migrate(asList("localhost"), "my_keyspace", asList(schemas));
```

The migrator will look for a `bootstrap.cql` file for setting up the keyspace.

## Standalone usage

```sh
$ java -Dhosts=localhost,192.168.1.1 -Dkeyspace=my_keyspace -Ddirectories=cql-common,cql-local -jar cqlmigrate.jar
```

## What it does

1. Checks all nodes are up and their schemas are in agreement.

2. Tries to acquire a lock for the keyspace. If it can't initially be acquired it will continue to retry at a set polling time until the timeout is reached.  

3. Looks for a `bootstrap.cql` file and runs it first. This file should contain the keyspace definition:

    ```
    CREATE KEYSPACE my_keyspace
     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
    ```        

4. Applies `.cql` files one by one, sorted by filename in ascending order. It is suggested to prefix
   the files with a datetime to order them:

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
This table is used to keep track of what locks are currently in place, and relies on
Cassandra's [lightweight transactions](https://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_ltwt_transaction_c.html).

    SELECT * FROM locks;
    
     name                                | client
    -------------------------------------+--------------------------------------
     airplanes_keyspace.schema_migration | 2a4ec2ae-d3d1-4b33-86a9-eb844e35eeeb
    
    (1 rows)

Each lock will be deleted by `cqlmigrate` once the migration is complete.

## Supported Cassandra versions

This project has been tested against the following versions:
* 2.1.7
* DSE 4.7.3 (2.1.8)
* 2.2.2
* 2.2.3

## Caveats

Cassandra is an eventually consistent, AP database, and so applying schema updates are not as simple
as a traditional relational database.

* Certain schema changes can cause data corruption on cassandra. Be very careful changing a schema for a
  table that is being actively written to. Generally, adding columns is safe (but be careful with
  collection types). Test the migration before rolling out to production.

* AP properties of Cassandra also apply to schema updates - so it is possible for a cluster to have an
  inconsistent schema across nodes in case of split brain or other situation. `cqlmigrate` tries to
  alleviate this with appropriate consistency levels.
 
# Contributors

Originally developed by the Cirrus team at Sky.

- Adam Dougal
- James Booth
- James Ravn
- Adrian Ng
- Malinda Rajapakse
- Ashutosh Gawande
- Dominic Mullings
- Yoseph Sultan
- David Sale
- Supreeth Rao
