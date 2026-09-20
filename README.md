# About

This tool was designed to handle Cassandra CQL compliant database migrations in a streaming fashion.

You can use it to migrate table data between
 * Cassandra <-> Cassandra
 * Cassandra <-> ScyllaDB
 * ScyllaDB <-> ScyllaDB

It is a standalone CLI jar driven by a YAML config: you define source/target DB and one or more table migrations, then it streams rows page-by-page (optional filters / transforms) into already-existing target tables — it does not migrate DDL. Table migrations can run in parallel (`threadCount`); within a table, writes can too.

For the technical overview see [docs/architecture-v1.2.md](docs/architecture-v1.2.md).

## Licensing

This code comes with [Apache License 2.0](https://opensource.org/licenses/Apache-2.0)

## Releases

You can check the [CHANGELOG.md](CHANGELOG.md) file for details.
 
# Requirements

The tool is writte in Java 8. So to run it you need
 * a JRE with Java8+ capabilities
 * the machine you run the tool must be able to access both the Source and the Target DB
 
# How to use

To use the tool:

1. Download the .jar (packaged with all dependencies) from our Nexus  
   Visit: https://nexus.keytiles.com/nexus/content/repositories/public-releases/com/keytiles/cassandra-cql-streaming-db-migrator/
   
1. Create a config file in .yaml format describing your migration setup  
   Take a look into the [migration-config.example.yaml](config/migration-config.example.yaml) file and the [config guide](docs-user/config-guide-v1.0.md)!
   
1. Execute the tool! See [docs-user/how-to-start-migration-v1.0.md](docs-user/how-to-start-migration-v1.0.md)  
   Short form: `java [-Xmx256M] -jar <jar> -configYaml <yaml> [-y]`  
   **Note:** if you run the tool in a machine sensitive to memory usage then really use the -Xmx (and/or other) Java options!  
   **Note:** the tool prints the prepared task list and asks for confirmation before starting. For `nohup` / non-interactive runs pass `-y` (or `--yes`).  
   Config walkthrough (including `${variables}` templates): [docs-user/config-guide-v1.0.md](docs-user/config-guide-v1.0.md).

# Tips

A few tips to keep in mind

 * To get the best latency and speed it makes sense to actually run the tool on one of the machines in the target DB cluster
   and configure `targetDB.contactNodes: localhost:9042`
 * Take advantage of the possibility `simulateOnly: true`! Especially if you have no clue how the migration would go but you do not
   want to accidentally mess up (yet) the target table  
