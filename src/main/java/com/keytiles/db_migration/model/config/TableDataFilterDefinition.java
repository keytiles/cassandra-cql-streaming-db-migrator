package com.keytiles.db_migration.model.config;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import com.keytiles.db_migration.api.IRowSetFilter;

/**
 * Sometimes you just want to migrate rows source => target if a certain criteria is met / not met.
 * Filtering is taking care of this.
 * <p>
 * One quick example to understand it better:
 * <ul>
 * <li>Assume we have TableNew and TableOld - with data. TableNew is a newer version of TableOld.
 * And the app
 * <ul>
 * <li>on Read-path: reading from TableNew, if not found then reading from TableOld
 * <li>on Write-path: writing to TableNew always
 * </ul>
 * ... so app is basically migrating data on-demand TableOld => TableNew
 * <li>And now we want to decommission TableOld
 * <li>To be able to do that we need to migrate all TableOld rows into TableNew where there is no
 * corresponding row yet in TableNew
 * </ul>
 * This is where filters come handy!
 *
 * @author AttilaW
 *
 */
public class TableDataFilterDefinition {

	/**
	 * Fully qualified class name of the filter class to use
	 */
	public String filterClass;

	/**
	 * Options map - to setup the concrete {@link IRowSetFilter} implementation class
	 */
	public Map<String, Object> filterOptions;

	/**
	 * Controls maximum how many rows the filter can handle efficiently. If not set then the
	 * {@link TableMigrationDefinition#pageSize} will be used
	 */
	public Integer maxRowsBatchSize;

	/**
	 * @return an instance of the configured plugin
	 */
	public IRowSetFilter getPluginInstance(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession) {
		IRowSetFilter filterInstance = null;
		try {
			Class<?> theClass = Class.forName(filterClass);
			Object filterCandidate = theClass.newInstance();
			Preconditions.checkArgument(filterCandidate instanceof IRowSetFilter,
					"class %s must be instance of %s but it looks it is not...", filterClass,
					IRowSetFilter.class.getSimpleName());
			filterInstance = (IRowSetFilter) filterCandidate;
			filterInstance.initialize(sourceTableMeta, targetTableMeta, tableMigrationDefinition, sourceDBSession,
					targetDbSession, filterOptions, maxRowsBatchSize);
		} catch (Exception e) {
			throw new IllegalStateException(
					"failed to create/initialize configured filter " + filterClass + " due to error: " + e.getMessage(),
					e);
		}
		return filterInstance;
	}
}
