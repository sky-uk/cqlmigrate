# Cassandra CQL migration tool

cqlmigrate is a library for performing schema migrations on a cassandra cluster.

It is best used as a dependency in your application, but can also be used
standalone.

## Library usage

The API is contained in `CqlMigrator`, and works on directories of files ending with `.cql`.

For example, to apply all `.cql` files located in `/cql` in the classpath:

    CqlMigrator migrator = new CqlMigrator();
    Path schemas = Paths.get(ClassLoader.getSystemResource("/cql").toURI());
    migrator.migrate(asList("localhost"), "my_keyspace", asList(schemas));

Examples of cql files can be seen in `src/test/resources`.

## Standalone usage 

```sh
$ java -Dhosts=localhost,192.168.1.1 -Dkeyspace=my_keyspace -Ddirectories=cql-common,cql-local -jar cqlmigrate.jar
```

## What it does

1. Looks for a `bootstrap.cql` file and runs it first. This file should contain your keyspace definition.

   For example:
   ```
   CREATE KEYSPACE my_keyspace
     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
   ```        

2. Applies `.cql` files one by one, sorted by filename in descending order. It is suggested you prefix
   your files with a datetime to order them.

   For example:
   ```
   /cql/2015-05-02-19:20-create-airplanes-table.cql
   /cql/2015-05-03-14:19-add-manufacturer-column.cql
   ```

   Any previously applied files will be skipped.

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

## Caveats

Cassandra is an eventually consistent, AP database, and so applying schema updates are not as simple
as a strongly consistent database.

* Schema migrations should only be run from a single client at a time. If you embed your migrations as
  part of application startup, you will need some sort of coordination system to prevent concurrent
  migrations. Consider using rolling updates, or a service such as zookeeper or etcd. 

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
- Yoseph Sultan