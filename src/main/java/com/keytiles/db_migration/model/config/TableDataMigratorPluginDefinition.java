package com.keytiles.db_migration.model.config;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import com.keytiles.db_migration.api.IMigratorPlugin;

public class TableDataMigratorPluginDefinition {

	/**
	 * Fully qualified class name of the {@link IMigratorPlugin} class to use
	 */
	public String migratorPluginClass;

	/**
	 * Options map - to setup the concrete {@link IMigratorPlugin} implementation class
	 */
	public Map<String, Object> migratorOptions;

	public TableDataMigratorPluginDefinition() {

	}

	public TableDataMigratorPluginDefinition(String migratorPluginClass, Map<String, Object> migratorOptions) {
		super();
		this.migratorPluginClass = migratorPluginClass;
		this.migratorOptions = migratorOptions;
	}

	public IMigratorPlugin getPluginInstance(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession) {
		IMigratorPlugin migratorInstance = null;
		try {
			Class<?> theClass = Class.forName(migratorPluginClass);
			Object migratorCandidate = theClass.newInstance();
			Preconditions.checkArgument(migratorCandidate instanceof IMigratorPlugin,
					"class %s must be instance of %s but it looks it is not...", migratorPluginClass,
					IMigratorPlugin.class.getSimpleName());
			migratorInstance = (IMigratorPlugin) migratorCandidate;
			migratorInstance.initialize(sourceTableMeta, targetTableMeta, tableMigrationDefinition, sourceDBSession,
					targetDbSession, migratorOptions);
		} catch (Exception e) {
			throw new IllegalStateException(
					"failed to create/initialize configured migrator plugin " + migratorPluginClass + " for "
							+ tableMigrationDefinition.tableName + " due to error: " + e.getMessage(),
					e);
		}

		return migratorInstance;
	}

}
