package com.keytiles.db_migration.implementation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Joiner;
import com.keytiles.db_migration.api.IRowSetFilter;
import com.keytiles.db_migration.model.config.TableDataFilterDefinition;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;
import com.keytiles.db_migration.util.CassandraSchemaUtil;

/**
 * This filter can be used to filter out Rows which already exist in the target table
 * <p>
 * This filter works based on actively querying the target table in a batch fashion using the full
 * PK columns set of the table. However please note: this filter can be very expensive... Assuming
 * table has N PK columns and we get M rows to filter the query looks like this:
 *
 * <pre>
 * SELECT pk1,pk2,...,pkN FROM <table> WHERE
 *    pk1 IN (row1.pk1, row2.pk1, ... rowM.pk1) AND
 *    pk2 IN (row1.pk2, row2.pk2, ... rowM.pk2) AND
 *    ...
 *    pkN IN (row1.pkN, row2.pkN, ... rowM.pkN);
 * </pre>
 *
 * so this could be really big! Complexity of query is N * M!
 *
 * <b>IMPORTANT! Because of the above complexity</b>
 * <ul>
 * <li>please heavily consider to use appropriate {@link TableDataFilterDefinition#maxRowsBatchSize}
 * parameter! Shrink it down to the necessary size especially if you have lots of PK columns!
 * <li>Make sure to put this filter to the last position in the
 * {@link TableMigrationDefinition#dataFilterDefinitions} array! So other filters (if you have) can
 * pre-filter rows faster
 * <li>see also the possibility: {@link TableMigrationDefinition#insertOnlyIfNotExist} - maybe you
 * can also use that?
 * </ul>
 *
 * @author AttilaW
 *
 */
public class IfNotExistFilter implements IRowSetFilter {

	private PreparedStatement readQuery;

	protected TableMetadata targetTableMeta;
	protected Set<String> targetTablePkColNames;

	public IfNotExistFilter() {
		throw new IllegalStateException("hey! this filter is not ready yet! Do not use it!");
	}

	@Override
	public void initialize(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession,
			Map<String, Object> options, Integer maxRowsBatchSize) {

		this.targetTableMeta = targetTableMeta;
		targetTablePkColNames = CassandraSchemaUtil.getTableKeyColumnNames(targetTableMeta);

		readQuery = createReadQuery(targetDbSession, tableMigrationDefinition);
	}

	private PreparedStatement createReadQuery(CqlSession targetDbSession,
			TableMigrationDefinition tableMigrationDefinition) {
		// let's compute which columns we will query
		String colNames = Joiner.on(", ").join(targetTablePkColNames);

		List<String> whereClauses = new ArrayList<>(targetTablePkColNames.size());
		for (String pkColName : targetTablePkColNames) {
			whereClauses.add(pkColName + " IN (:" + pkColName + ")");
		}

		StringBuilder queryString = new StringBuilder("select ") //
				.append(colNames) //
				.append(" from ") //
				.append(tableMigrationDefinition.getTargetTableName()) //
				.append(" where ").append(Joiner.on(" AND ").join(whereClauses));

		PreparedStatement readQuery = targetDbSession.prepare(queryString.toString());
		return readQuery;
	}

	@Override
	public List<Row> filterRowSet(List<Row> rows) {
		return rows;
	}

}
