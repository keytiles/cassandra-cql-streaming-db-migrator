package com.keytiles.db_migration.api;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.keytiles.db_migration.model.config.TableDataFilterDefinition;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;

/**
 * Classes who are implementing data filtering must implement this interface.
 * <p>
 * See {@link TableDataFilterDefinition} for more details
 *
 * @author AttilaW
 *
 */
public interface IRowSetFilter {

	/**
	 * Method is invoked right after instantiation to pass on options defined in
	 * {@link TableDataFilterDefinition#filterOptions} so instance can setup
	 *
	 * @param options
	 *            the options from the config file - can be NULL
	 */
	public void initialize(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession,
			Map<String, Object> options, Integer maxRowsBatchSize);

	/**
	 * The method gets a list (order might be important) rows, filter it, then returns the rows passed
	 * the filter
	 *
	 * @param rows
	 *            the set of rows to filter
	 * @return the rows passed the filter
	 */
	public List<Row> filterRowSet(List<Row> rows);
}
