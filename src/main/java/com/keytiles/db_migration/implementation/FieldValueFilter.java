package com.keytiles.db_migration.implementation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.mvel2.MVEL;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.keytiles.db_migration.api.IRowSetFilter;
import com.keytiles.db_migration.model.config.TableDataFilterDefinition;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;
import com.keytiles.db_migration.util.CassandraSchemaUtil;
import com.keytiles.db_migration.util.CassandraSchemaUtil.JavaTypeInfo;

/**
 * With this filter you can create in-memory checks for multiple fields.
 * <p>
 * In the {@link TableDataFilterDefinition#filterOptions}, under key {@value #OPTION_MVEL_RULE} map
 * you have to pass on an MVEL rule in string format to describe the filter. The rows making the
 * rule TRUE will pass the filter
 * <p>
 * You can refer to different columns by writing "row.{column_name}" in the rule
 *
 * @author AttilaW
 *
 */
public class FieldValueFilter implements IRowSetFilter {

	public final static String OPTION_MVEL_RULE = "mvelRule";

	protected String mvelRule;
	protected Serializable compiledMvelRule;
	protected Set<String> columnNamesInRule;
	protected Map<String, JavaTypeInfo> sourceTableColumnJavaTypes;

	public FieldValueFilter() {
	}

	@Override
	public void initialize(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession,
			Map<String, Object> options, Integer maxRowsBatchSize) {

		mvelRule = (String) options.get(OPTION_MVEL_RULE);
		Preconditions.checkArgument(StringUtils.isNotBlank(mvelRule), "Option '%s' can not be blank!",
				OPTION_MVEL_RULE);

		columnNamesInRule = CassandraSchemaUtil.findRowColumnReferencesInString(mvelRule);
		Preconditions.checkArgument(!columnNamesInRule.isEmpty(),
				"There are no column names found in the '%s' you added to table '%s' and this does not make sense! Did you write thewm in 'row.<column_name>' format?? The rule was: %s",
				OPTION_MVEL_RULE, tableMigrationDefinition.tableName, mvelRule);
		// let's get rid of the "row." markers so we will have easier life in matching later
		mvelRule = mvelRule.replace(CassandraSchemaUtil.ROW_COLUMN_STRING_PREFIX, "");
		compiledMvelRule = MVEL.compileExpression(mvelRule);

		sourceTableColumnJavaTypes = CassandraSchemaUtil.getColJavaTypesByColumnNames(sourceTableMeta);
	}

	@Override
	public List<Row> filterRowSet(List<Row> rows) {
		List<Row> filteredRows = new ArrayList<>(rows.size());

		// we need to put together the objects
		Map<String, Object> rowExtract = new HashMap<>();
		for (Row row : rows) {
			rowExtract.clear();

			for (String colName : columnNamesInRule) {
				Object value = row.get(colName, sourceTableColumnJavaTypes.get(colName).mainType);
				rowExtract.put(colName, value);
			}
			Boolean result = (Boolean) MVEL.executeExpression(compiledMvelRule, rowExtract);
			if (result) {
				filteredRows.add(row);
			}
		}

		return filteredRows;
	}

}
