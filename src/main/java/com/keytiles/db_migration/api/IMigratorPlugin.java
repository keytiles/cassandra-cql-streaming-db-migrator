package com.keytiles.db_migration.api;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;

/**
 * Implementors are taking part into the process the way they
 * <ul>
 * <li>they are building up the read query
 * <li>they are responsible for migrating one row
 * </ul>
 *
 * @author AttilaW
 *
 */
public interface IMigratorPlugin {

	public void initialize(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession,
			Map<String, Object> migratorOptions);

	public SimpleStatement getReadQuery();

	/**
	 * Migrates a row
	 *
	 * @param row
	 *            the row to migrate
	 * @return TRUE if the migration was performed so row was written - FALSE otherwise
	 */
	public boolean migrateRow(Row row);

	public List<String> getWarningMessages();
}
