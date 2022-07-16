package com.keytiles.db_migration.model.config;

import java.util.List;

import com.keytiles.db_migration.api.IMigratorPlugin;
import com.keytiles.db_migration.implementation.DefaultMigratorPlugin;
import com.keytiles.db_migration.implementation.IfNotExistFilter;
import com.keytiles.db_migration.model.BaseEntity;

/**
 * Defines a table to be migrated. Attributes can be used to fine-tune how the migration deals with
 * the table data
 *
 * @author AttilaW
 *
 */
public class TableMigrationDefinition extends BaseEntity {

	/**
	 * Optionally you can assign a name to this migration - this is used in logs/errors etc.
	 * <p>
	 * If you do not assign one directly then it will be calculated
	 */
	public String name;

	/**
	 * This is the name of the table in the source db we want to migrate.
	 */
	public String tableName;

	/**
	 * Optional setting. This is the name of the table in the target db we want the data to migrate to.
	 * If not set then "tableName" is used
	 * <p>
	 * To put it this way: it is possible to migrate data sourcedb.MyTableA => targetdb.MyTableB
	 * (assuming MyTableB is following a compatible schema with MyTableA)
	 */
	public String targetTableName;

	/**
	 * Controls if the migration should be simulated only or real. As you can see this is set to TRUE by
	 * default because this is the safest option to have (a half exited migration on large data can kill
	 * us really especially if we can not just simply redo the stuff)
	 * <p>
	 * In simulation mode the migrator will do every operation exactly the same as it would, except one
	 * thing: the UPSERT query itself (however will be parametrized too with row data) will be skipped.
	 */
	public boolean simulateOnly = true;

	/**
	 * If you want to limit the query just add your where clause here as a string
	 * <p>
	 * e.g. "pk1={value} AND pk2 IN ('{value1', '{value2}')
	 */
	public String whereClause;
	/**
	 * Mainly for migration test purposes you can limit the number of rows to X for reading from source
	 * (this will add a 'limit X' clause to the read query)
	 */
	public int maxReadRowCount = -1;
	/**
	 * Mainly for migration test purposes you can limit the number of rows to X for writing to target
	 * (this will abort the process once migrated row count reaches this limit)
	 */
	public int maxWriteRowCount = -1;

	/**
	 * Controls whether migrator should try to migrate records with TTL to keep that.
	 */
	public boolean respectTTL = true;

	/**
	 * Select query will use this page size during iterating through
	 */
	public int pageSize = 1000;

	/**
	 * How long the cassandra driver (as client) will wait for the read query
	 */
	public long readQueryTimeoutMillis = 20000;

	/**
	 * How long the cassandra driver (as client) will wait for the write query
	 */
	public long writeQueryTimeoutMillis = 20000;

	/**
	 * If you want to "slow down" the migration a bit by not putting too much read-load on the source DB
	 * you can apply a slight pause btw page fetches
	 */
	public long pauseMillisBetweenPages = 0;

	/**
	 * Optional filters to use over the table data during the migration
	 * <p>
	 * The filters are executed in order - forming up a chain if multiple is given
	 */
	public List<TableDataFilterDefinition> dataFilterDefinitions;

	/**
	 * If TRUE then migrator migrates the record only if it does not exist in the target table. It is
	 * implemented the way migrator is using INSERT ... IF NOT EXIST statements instead of upserts
	 * <p>
	 * <b>IMPORTANT NOTES!</b>
	 * <ul>
	 * <li>this does not work on a counter tables... Because they do not support INSERT method
	 * <li>also you need SERIAL / LOCAL_SERIAL consistency level achievable - which is basically QUORUM
	 * therefore can be problematic with clusters RF < 3 ...
	 * </ul>
	 * If you run into failures / issues you might consider using the {@link IfNotExistFilter}!
	 */
	public boolean insertOnlyIfNotExist = false;

	/**
	 * The {@link IMigratorPlugin} to use for this migration. The default is
	 * {@link DefaultMigratorPlugin}
	 */
	public TableDataMigratorPluginDefinition migratorPluginDefinition = new TableDataMigratorPluginDefinition(
			DefaultMigratorPlugin.class.getName(), null);

	/**
	 * If a row migration fails what to do? Abort the full stuff? Or continue?
	 */
	public Boolean continueOnRowError;

	public String getTargetTableName() {
		if (targetTableName != null) {
			return targetTableName;
		}
		return tableName;
	}
}
