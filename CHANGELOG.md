
# release 1.1.0

## Breaking changes:
none

## New features:
 * Enhancing parallel writes
    * Introduced TableMigrationDefinition.parallelWriteRowCount
    * Main config 'threadCount' now taking one configured TableMigrationDefinition and deals with that.
 
## Bug fixes:
 * TableMigrationDefinition.respectTTL did not do anything - now it really works


# release 1.0.1

## Breaking changes:
none

## New features:
none
 
## Bug fixes:
 * Execution of table migrations was not following the order how they were defined in config .yaml `table:` section  


# release 1.0.0

Initial release
