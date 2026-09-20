
# release 1.2.0

## Improvements
 * Added `agents` folder and all other folders we use for AI assisted dev cycles. See https://github.com/keytiles/ai-agents for more details.

## New features:
 * Improving final stat printing - until now we printed the tableName and the stats but thats bad as often we use same tables in a migration task just with different whereClause or filter.
   From now we print the migration task name - which even gets the index as defined in the .yaml config file so better identify.
 * Introducing `TableMigrationDefinition.writeRetryStrategy`. Earlier we captured many times that certain rows were not migrated because of a probably temporary issue. But instead of waiting there
   a bit we moved on. This is especially annoying when we have a huge amount of data, worst is counter tables like time series. See `migration-config.example.yaml` for more details!
 * Introducing timeout configurability into config.sourceDB / config.targetDB - so far it was only constant 10s. See `migration-config.example.yaml` for more details!
 * Resilient source reads in `MigrateTableTask`: manual paging (paging state), default `readRetryStrategy`, and default `pageSizeReduceStrategy` so a mid-stream read timeout no longer aborts multi-million-row migrations. See `migration-config.example.yaml`.
 * Table migration **task template** DSL: `${placeholders}` + `variables` lists expand (cartesian product, YAML-text substitute) into multiple concrete table tasks. Cap via `maxVariablesCartesianProductsPerTask` (default 16). See `docs/feature-TableMigrationTaskTemplate-v1.0.md`.
 * Pre-start task-list confirmation (Y/N). Unattended/`nohup` runs must pass `-y` / `--yes`.
 * Operator guides under `docs-user/`: how to start a migration, and a progressive YAML config guide (including `${variables}` templates).

## Bug fixes:
 * `ThreadUtil.waitMillis` now restores the thread interrupt flag after `InterruptedException` (so shutdown/cancel can take effect).

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
