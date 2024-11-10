package com.keytiles.db_migration.implementation;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.mvel2.MVEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.google.common.base.Joiner;
import com.keytiles.db_migration.api.IMigratorPlugin;
import com.keytiles.db_migration.model.BaseEntity;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;
import com.keytiles.db_migration.util.CassandraSchemaUtil;
import com.keytiles.db_migration.util.CassandraSchemaUtil.JavaTypeInfo;

public class DefaultMigratorPlugin implements IMigratorPlugin {

	private final static Logger LOG = LoggerFactory.getLogger(DefaultMigratorPlugin.class);

	/**
	 * Plugin is capable of add extra columns to the migration using formula evaluated with the inbound
	 * Row in the {@link #migrateRow(Row)} method. To do that you can add option
	 * {@value #OPTION_CALCULATED_COLUMNS} which is a list of objects in the following form:
	 *
	 * <pre>
	 * migratorPluginDefinition:
	 *   migratorPluginClass: com.keytiles.db_migration.implementation.DefaultMigratorPlugin
	 *   migratorOptions:
	 *     ...
	 *     {@value #OPTION_CALCULATED_COLUMNS}:
	 *     - columnName: {name of your calculated column}
	 *       mvelExpression: {MVEL expression - you can use row.{colName} inside}
	 *     - columnName: {name of your calculated column}
	 *       mvelExpression: {MVEL expression - you can use row.{colName} inside}
	 *     ...
	 * </pre>
	 */
	public final static String OPTION_CALCULATED_COLUMNS = "calculatedColumns";
	public final static String CALCULATED_COLUMNS_OPTION_COLNAME = "columnName";
	public final static String CALCULATED_COLUMNS_OPTION_MVELEXPRESSION = "mvelExpression";

	protected final static String TTL_COL_NAME = "row_ttl_value";

	protected static class ColumnCalculation extends BaseEntity {
		public Serializable mvelExpression;
		public Set<String> usedSrcRowColumns;
	}

	protected TableMetadata sourceTableMeta;
	protected TableMetadata targetTableMeta;
	protected TableMigrationDefinition tableMigrationDefinition;
	protected CqlSession sourceDBSession;
	protected CqlSession targetDbSession;

	protected SimpleStatement readQuery;
	protected Set<String> columnsUsedInReadQuery;
	protected PreparedStatement writeQuery;
	protected Set<String> columnsUsedInWriteQuery;

	protected Map<String, JavaTypeInfo> sourceTableColumnJavaTypes;
	protected Map<String, JavaTypeInfo> targetTableColumnJavaTypes;
	protected boolean isTargetCounterTable;

	/**
	 * The name of the column used to determine the TTL of row read from the source or NULL if TTL
	 * calculation is not happening
	 */
	protected String ttlCalculationColumn;
	protected int targetTableTTL;
	protected int sourceTableTTL;

	/**
	 * This is created from {@link #OPTION_CALCULATED_COLUMNS} - if it was there.
	 */
	protected Map<String, ColumnCalculation> calculatedColumns;

	protected List<String> warningMessages = new LinkedList<>();

	public DefaultMigratorPlugin() {
	}

	@Override
	public void initialize(TableMetadata sourceTableMeta, TableMetadata targetTableMeta,
			TableMigrationDefinition tableMigrationDefinition, CqlSession sourceDBSession, CqlSession targetDbSession,
			Map<String, Object> migratorOptions) {

		this.sourceTableMeta = sourceTableMeta;
		this.targetTableMeta = targetTableMeta;
		this.tableMigrationDefinition = tableMigrationDefinition;
		this.sourceDBSession = sourceDBSession;
		this.targetDbSession = targetDbSession;

		this.calculatedColumns = assembleCalculatedColumnsMap(migratorOptions);

		if (tableMigrationDefinition.simulateOnly) {
			warningMessages.add("Just running in simulation mode! Nothing will be/was migrated!");
		}

		sourceTableColumnJavaTypes = CassandraSchemaUtil.getColJavaTypesByColumnNames(sourceTableMeta);
		targetTableColumnJavaTypes = CassandraSchemaUtil.getColJavaTypesByColumnNames(targetTableMeta);
		isTargetCounterTable = CassandraSchemaUtil.isCounterTable(targetTableMeta);

		if (tableMigrationDefinition.respectTTL) {
			sourceTableTTL = CassandraSchemaUtil.getTableLevelTTL(sourceTableMeta);
			targetTableTTL = CassandraSchemaUtil.getTableLevelTTL(targetTableMeta);
		}

		// order is important here!
		// as createReadQuery will control ttlCalculationColumn field
		readQuery = createReadQuery();
		writeQuery = createWriteQuery();
	}

	protected Map<String, ColumnCalculation> assembleCalculatedColumnsMap(Map<String, Object> migratorOptions) {
		Map<String, ColumnCalculation> calculatedColumnsMap = new HashMap<>();
		Object calculatedColumnsOption = migratorOptions != null ? migratorOptions.get(OPTION_CALCULATED_COLUMNS)
				: null;
		if (calculatedColumnsOption != null) {
			Preconditions.checkArgument(calculatedColumnsOption instanceof List<?>,
					"option '%s' should be a List but that is %s", OPTION_CALCULATED_COLUMNS,
					calculatedColumnsOption.getClass());

			Set<String> targetTableColNames = CassandraSchemaUtil.getTableColumnNames(targetTableMeta);

			List<Map<String, Object>> calculatedColumns = (List<Map<String, Object>>) calculatedColumnsOption;
			for (Map<String, Object> optionItem : calculatedColumns) {
				ColumnCalculation calcStruct = new ColumnCalculation();
				String mvelExpression = (String) optionItem.get(CALCULATED_COLUMNS_OPTION_MVELEXPRESSION);
				String columnName = (String) optionItem.get(CALCULATED_COLUMNS_OPTION_COLNAME);

				// quick checks!
				// the column name (at least for now) must be a column name in target table
				Preconditions.checkArgument(targetTableColNames.contains(columnName),
						"calculated column '%s' is not found in target table columns - for now we only allow calculated columns for that",
						columnName);
				Preconditions.checkState(!calculatedColumnsMap.containsKey(columnName),
						"Problem in '%s' option: columnName '%s' is used multiple times which is probably a typo... so please check the config!",
						OPTION_CALCULATED_COLUMNS, columnName);

				calcStruct.usedSrcRowColumns = CassandraSchemaUtil.findRowColumnReferencesInString(mvelExpression);
				// let's get rid of the "row." markers so we will have easier life in matching later
				mvelExpression = mvelExpression.replace(CassandraSchemaUtil.ROW_COLUMN_STRING_PREFIX, "");
				calcStruct.mvelExpression = MVEL.compileExpression(mvelExpression);

				calculatedColumnsMap.put(columnName, calcStruct);
			}

		}
		return calculatedColumnsMap;
	}

	@Override
	public SimpleStatement getReadQuery() {
		return readQuery;
	}

	protected SimpleStatement createReadQuery() {
		// let's compute which columns we will query
		Set<String> columnsToQuery = CassandraSchemaUtil.getTableColumnNames(sourceTableMeta);
		columnsUsedInReadQuery = new HashSet<>(columnsToQuery);

		Set<String> sourceTablePkColNames = CassandraSchemaUtil.getTableKeyColumnNames(sourceTableMeta);
		Set<String> sourceTableNonPkColNames = CassandraSchemaUtil.getTableColumnNames(sourceTableMeta);
		sourceTableNonPkColNames.removeAll(sourceTablePkColNames);
		Set<String> targetTablePkColNames = CassandraSchemaUtil.getTableKeyColumnNames(targetTableMeta);
		Set<String> targetTableNonPkColNames = CassandraSchemaUtil.getTableColumnNames(targetTableMeta);
		targetTableNonPkColNames.removeAll(targetTablePkColNames);

		if (targetTableTTL > 0 && sourceTableTTL > 0) {
			LOG.debug("target table has TTL = {}, source table TTL = {} - kicking in TTL respect", targetTableTTL,
					sourceTableTTL);
			// we need a column for TTL calculation
			// for now take one data column ...
			for (String colName : sourceTableNonPkColNames) {
				ttlCalculationColumn = colName;
				break;
			}
			columnsToQuery.add("TTL(" + ttlCalculationColumn + ") as " + TTL_COL_NAME);
			columnsUsedInReadQuery.add(TTL_COL_NAME);
		}
		String colNames = Joiner.on(", ").join(columnsToQuery);

		String queryString = "select " + colNames + " from " + sourceTableMeta.getName().asInternal();
		if (StringUtils.isNotBlank(tableMigrationDefinition.whereClause)) {
			// let's add the where clause!
			queryString += " where " + tableMigrationDefinition.whereClause;
		}
		if (tableMigrationDefinition.maxReadRowCount > 0) {
			// let's add the limit clause
			queryString += " limit " + tableMigrationDefinition.maxReadRowCount;
		}
		SimpleStatement readQuery = new SimpleStatementBuilder(queryString)
				.setPageSize(tableMigrationDefinition.pageSize) //
				.setTimeout(Duration.ofMillis(tableMigrationDefinition.readQueryTimeoutMillis)) //
				.build();
		return readQuery;
	}

	@Override
	public List<String> getWarningMessages() {
		return new ArrayList<>(warningMessages);
	}

	protected PreparedStatement createWriteQuery() {

		String targetTableName = tableMigrationDefinition.getTargetTableName();

		boolean isTargetCounterTable = CassandraSchemaUtil.isCounterTable(targetTableMeta);
		// boolean isSourceCounterTable = CassandraSchemaUtil.isCounterTable(sourceTableMeta);
		// if source table is counter table then target also must be
		// Preconditions.checkState(
		// CassandraSchemaUtil.isCounterTable(sourceTableMeta) == CassandraSchemaUtil
		// .isCounterTable(targetTableMeta),
		// "sourceTable and targetTable either both are counter tables or none of them!");

		Set<String> sourceTableColumnNames = CassandraSchemaUtil.getTableColumnNames(sourceTableMeta);
		Set<String> targetTablePkColNames = CassandraSchemaUtil.getTableKeyColumnNames(targetTableMeta);
		Set<String> targetTableNonPkColNames = CassandraSchemaUtil.getTableColumnNames(targetTableMeta);
		targetTableNonPkColNames.removeAll(targetTablePkColNames);

		// if there is column in the target table which is
		// - not in the source table
		// - neither in the calculated columns
		// then...
		// if it is in PK then suxxx...
		Set<String> helper = new HashSet<>(targetTablePkColNames);
		helper.removeAll(sourceTableColumnNames);
		helper.removeAll(calculatedColumns.keySet());
		Preconditions.checkState(helper.isEmpty(),
				"There are PK columns in target table '%s' which are not present in the source table '%s' and neither in the set of calculated columns! This is not allowed! Problematic column names are: %s",
				targetTableName, tableMigrationDefinition.tableName, helper);
		// if it is in non-pk column then we simply leave that out
		helper = new HashSet<>(targetTableNonPkColNames);
		helper.removeAll(sourceTableColumnNames);
		helper.removeAll(calculatedColumns.keySet());
		if (!helper.isEmpty()) {
			String warnMsg = "There are columns in target table '" + targetTableName
					+ "' which are not present in source table '" + tableMigrationDefinition.tableName
					+ "' and neither in the set of calculated columns! These columns will be skipped during migration. These columns are: "
					+ helper;
			LOG.warn(warnMsg);
			warningMessages.add(warnMsg);

			// OK let's really exclude them
			targetTableNonPkColNames.removeAll(helper);
		}

		StringBuilder cql = new StringBuilder();

		columnsUsedInWriteQuery = new LinkedHashSet<>(targetTablePkColNames);
		columnsUsedInWriteQuery.addAll(targetTableNonPkColNames);

		if (tableMigrationDefinition.insertOnlyIfNotExist) {

			// we need to go with INSERT statement

			cql //
					.append("INSERT INTO ") //
					.append(targetTableName) //
					.append(" (") //
					.append(Joiner.on(", ").join(columnsUsedInWriteQuery)) //
					.append(") VALUES (:") //
					.append(Joiner.on(", :").join(columnsUsedInWriteQuery)) //
					.append(") IF NOT EXISTS");

			// do we deal with TTL?
			if (ttlCalculationColumn != null) {
				cql.append(" USING TTL :" + TTL_COL_NAME);
			}

		} else {

			// we build an UPDATE statement

			cql //
					.append("UPDATE ") //
					.append(targetTableName); //
			// do we deal with TTL?
			if (ttlCalculationColumn != null) {
				cql.append(" USING TTL :" + TTL_COL_NAME);
			}
			cql.append(" SET ");

			List<String> nonPkAssignments = new ArrayList<>();
			for (String colName : targetTableNonPkColNames) {
				if (isTargetCounterTable) {
					nonPkAssignments.add(colName + "=" + colName + "+:" + colName);
				} else {
					nonPkAssignments.add(colName + "=:" + colName);
				}
			}

			List<String> pkAssignments = new ArrayList<>();
			for (String colName : targetTablePkColNames) {
				pkAssignments.add(colName + "=:" + colName);
			}

			cql //
					.append(Joiner.on(", ").join(nonPkAssignments)) //
					.append(" WHERE ") //
					.append(Joiner.on(" AND ").join(pkAssignments));
		}

		try {
			PreparedStatement ps = targetDbSession.prepare(cql.toString());
			return ps;
		} catch (Exception e) {
			throw new IllegalStateException(
					"createWriteQuery() failed with exception for query '" + cql.toString() + "' ! exception was: " + e,
					e);
		}
	}

	@Override
	public boolean migrateRow(Row row) {

		// TODO migrate row
		BoundStatementBuilder bs = writeQuery.boundStatementBuilder();

		bs.setTimeout(Duration.ofMillis(tableMigrationDefinition.writeQueryTimeoutMillis));

		Set<String> columnsNotAssigned = new HashSet<>();
		for (String colName : columnsUsedInWriteQuery) {
			JavaTypeInfo javaTypeInfo = targetTableColumnJavaTypes.get(colName);
			try {
				Object genericValue = row.get(colName, javaTypeInfo.mainType);
				setColumnInBoundStatement(colName, genericValue, javaTypeInfo, bs);
			} catch (IllegalArgumentException e) {
				if (e.getMessage().contains("not a column in this row")) {
					columnsNotAssigned.add(colName);
				} else {
					throw e;
				}
			}
		}

		if (ttlCalculationColumn != null) {
			int rowTtl = row.getInt(TTL_COL_NAME);
			int rowAge = sourceTableTTL - rowTtl;
			int targetRowTtl = targetTableTTL - rowAge;
			bs.setInt(TTL_COL_NAME, targetRowTtl);
		}

		// add the calculated columns
		Map<String, Object> rowCols = new HashMap<>();
		for (Map.Entry<String, ColumnCalculation> calcColEntry : calculatedColumns.entrySet()) {
			rowCols.clear();
			String calcColName = calcColEntry.getKey();

			for (String colName : calcColEntry.getValue().usedSrcRowColumns) {
				Object value = row.get(colName, sourceTableColumnJavaTypes.get(colName).mainType);
				rowCols.put(colName, value);
			}
			// let's compute the value
			Object calcColValue = MVEL.executeExpression(calcColEntry.getValue().mvelExpression, rowCols);
			// type check - can we write this into the target table?
			JavaTypeInfo targetTableColTypeInfo = targetTableColumnJavaTypes.get(calcColName);
			Preconditions.checkArgument(targetTableColTypeInfo.mainType.isAssignableFrom(calcColValue.getClass()),
					"Oops! Calculated column '%s' returned class %s but target table column class is %s... they are not compatible!",
					calcColName, calcColValue.getClass(), targetTableColTypeInfo.mainType);
			// let's write into the target table!
			setColumnInBoundStatement(calcColName, calcColValue, targetTableColTypeInfo, bs);
			columnsNotAssigned.remove(calcColName);
		}

		// by now we should have all values in write query assigned
		Preconditions.checkState(columnsNotAssigned.isEmpty(),
				"not all columns in write query has assigned value! missing columns: %s", columnsNotAssigned);

		// we execute the query only if not in simulation mode
		boolean wasMigrated = false;
		if (!tableMigrationDefinition.simulateOnly) {
			// bs.setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
			bs.setConsistencyLevel(ConsistencyLevel.ONE);
			bs.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
			ResultSet rs = targetDbSession.execute(bs.build());
			wasMigrated = rs.wasApplied();
		}

		return wasMigrated;
	}

	/**
	 * The {@link BoundStatement} setters are pretty rigid in terms of types so we extracted this into a
	 * standalone method as we need lots of if statements.. :-(
	 *
	 * @param colName
	 * @param genericValue
	 * @param javaTypeInfo
	 * @param bs
	 */
	private void setColumnInBoundStatement(String colName, Object genericValue, JavaTypeInfo javaTypeInfo,
			BoundStatementBuilder bs) {
		if (Short.class.equals(javaTypeInfo.mainType)) {
			Short value = (Short) genericValue;
			bs.set(colName, value, Short.class);
		} else if (Byte.class.equals(javaTypeInfo.mainType)) {
			Byte value = (Byte) genericValue;
			bs.set(colName, value, Byte.class);
		} else if (Integer.class.equals(javaTypeInfo.mainType)) {
			Integer value = (Integer) genericValue;
			bs.set(colName, value, Integer.class);
		} else if (Long.class.equals(javaTypeInfo.mainType)) {
			Long value = (Long) genericValue;
			// this is a bit tricky because this is the datatype of COUNTER columns
			// where formula is col = col+<value> so here we have to skip if value is NULL!
			if (value != null || !"counter".equals(javaTypeInfo.mainCqlType)) {
				bs.set(colName, value, Long.class);
			}
		} else if (Float.class.equals(javaTypeInfo.mainType)) {
			Float value = (Float) genericValue;
			bs.set(colName, value, Float.class);
		} else if (Double.class.equals(javaTypeInfo.mainType)) {
			Double value = (Double) genericValue;
			bs.set(colName, value, Double.class);
		} else if (BigDecimal.class.equals(javaTypeInfo.mainType)) {
			BigDecimal value = (BigDecimal) genericValue;
			bs.set(colName, value, BigDecimal.class);
		} else if (String.class.equals(javaTypeInfo.mainType)) {
			String value = (String) genericValue;
			bs.set(colName, value, String.class);
		} else if (Boolean.class.equals(javaTypeInfo.mainType)) {
			Boolean value = (Boolean) genericValue;
			bs.set(colName, value, Boolean.class);
		} else if (UUID.class.equals(javaTypeInfo.mainType)) {
			UUID value = (UUID) genericValue;
			bs.set(colName, value, UUID.class);
		} else if (List.class.equals(javaTypeInfo.mainType)) {
			List value = (List) genericValue;
			bs.setList(colName, value, javaTypeInfo.valueType);
		} else if (Set.class.equals(javaTypeInfo.mainType)) {
			Set value = (Set) genericValue;
			bs.setSet(colName, value, javaTypeInfo.valueType);
		} else if (Map.class.equals(javaTypeInfo.mainType)) {
			Map value = (Map) genericValue;
			bs.setMap(colName, value, javaTypeInfo.keyType, javaTypeInfo.valueType);
		} else {
			throw new IllegalStateException(
					"Oops! No clue how to map java type " + javaTypeInfo + " into target row...");
		}
	}

}
