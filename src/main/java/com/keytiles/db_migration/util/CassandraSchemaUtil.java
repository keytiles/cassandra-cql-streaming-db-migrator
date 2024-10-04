package com.keytiles.db_migration.util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.keytiles.db_migration.model.BaseEntity;

public class CassandraSchemaUtil {

	public final static String ROW_COLUMN_STRING_PREFIX = "row.";
	private final static Pattern COLUMN_NAME_MATCHER = Pattern
			.compile(Pattern.quote(ROW_COLUMN_STRING_PREFIX) + "(?<columnName>[a-zA-Z0-9_]*)");

	public static class JavaTypeInfo extends BaseEntity {
		public final Class<?> mainType;
		public final String mainCqlType;
		public final Class<?> keyType;
		public final Class<?> valueType;

		public JavaTypeInfo(Class<?> mainType, String mainCqlType) {
			this(mainType, mainCqlType, null, null);
		}

		public JavaTypeInfo(Class<?> mainType, String mainCqlType, Class<?> keyType, Class<?> valueType) {
			this.mainType = mainType;
			this.mainCqlType = mainCqlType;
			this.keyType = keyType;
			this.valueType = valueType;
		}
	}

	private CassandraSchemaUtil() {
	}

	/**
	 * Searches for "row.{columnName}" strings in the given string and returns the {columName}s in a set
	 *
	 * @param str
	 *            the string to search over
	 * @return the found column names
	 */
	public static Set<String> findRowColumnReferencesInString(String str) {
		Set<String> rowColumnNamesInString = new HashSet<>();
		Matcher m = COLUMN_NAME_MATCHER.matcher(str);
		while (m.find()) {
			rowColumnNamesInString.add(m.group("columnName"));
		}
		return rowColumnNamesInString;
	}

	/**
	 * Checks if a table is a counter table. This means it has COUNTER type column
	 *
	 * @return TRUE if table is counter table
	 */
	public static boolean isCounterTable(TableMetadata tableMeta) {
		for (Map.Entry<CqlIdentifier, ColumnMetadata> colMetaEntry : tableMeta.getColumns().entrySet()) {
			DataType type = colMetaEntry.getValue().getType();
			if (type instanceof PrimitiveType) {
				boolean includeFrozen = false;
				boolean pretty = false;
				if ("counter".equalsIgnoreCase(((PrimitiveType) type).asCql(includeFrozen, pretty))) {
					// we found a counter column - so table is counter table
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * @return the "default_time_to_live" value of the table
	 */
	public static int getTableLevelTTL(TableMetadata tableMeta) {
		Object ttlValue = tableMeta.getOptions().get(CqlIdentifier.fromCql("default_time_to_live"));
		if (ttlValue == null) {
			return 0;
		}
		return ((Integer) ttlValue).intValue();
	}

	/**
	 * @return all column names of the table
	 */
	public static Set<String> getTableColumnNames(TableMetadata tableMeta) {
		Set<String> colNames = new LinkedHashSet<>();
		for (Map.Entry<CqlIdentifier, ColumnMetadata> colMetaEntry : tableMeta.getColumns().entrySet()) {
			colNames.add(colMetaEntry.getKey().asInternal());
		}
		return colNames;
	}

	/**
	 * @return column names of the partitioning key columns
	 */
	public static Set<String> getTablePartitioningKeyColumnNames(TableMetadata tableMeta) {
		Set<String> colNames = new LinkedHashSet<>();
		for (ColumnMetadata colMeta : tableMeta.getPartitionKey()) {
			colNames.add(colMeta.getName().asInternal());
		}
		return colNames;
	}

	/**
	 * @return column names of the partitioning+clustering key columns
	 */
	public static Set<String> getTableKeyColumnNames(TableMetadata tableMeta) {
		Set<String> colNames = new LinkedHashSet<>();
		for (ColumnMetadata colMeta : tableMeta.getPrimaryKey()) {
			colNames.add(colMeta.getName().asInternal());
		}
		return colNames;
	}

	/**
	 * @return column names of the clustering key columns
	 */
	public static Set<String> getTableClusteringColumnNames(TableMetadata tableMeta) {
		Set<String> colNames = getTableKeyColumnNames(tableMeta);
		colNames.removeAll(getTablePartitioningKeyColumnNames(tableMeta));
		return colNames;
	}

	/**
	 *
	 * @return columnName => Java class format map
	 */
	public static Map<String, JavaTypeInfo> getColJavaTypesByColumnNames(TableMetadata tableMeta) {

		Map<String, JavaTypeInfo> jTypes = new HashMap<>();

		for (Map.Entry<CqlIdentifier, ColumnMetadata> colMetaEntry : tableMeta.getColumns().entrySet()) {
			String columnName = colMetaEntry.getKey().asInternal();
			DataType type = colMetaEntry.getValue().getType();
			JavaTypeInfo typeInfo = getJavaTypeForCassandraType(type);

			if (typeInfo == null) {
				throw new RuntimeException("table " + tableMeta.getName() + "." + columnName
						+ " column has not supported type by the migrator! Type: " + type + ", class: "
						+ type.getClass());
			}

			jTypes.put(columnName, typeInfo);
		}

		return jTypes;
	}

	public static JavaTypeInfo getJavaTypeForCassandraType(DataType type) {
		JavaTypeInfo typeInfo = null;
		boolean includeFrozen = false;
		boolean pretty = false;
		String cqlType = type.asCql(includeFrozen, pretty);
		if (type instanceof ListType) {
			JavaTypeInfo valueTypeInfo = getJavaTypeForCassandraType(((ListType) type).getElementType());
			typeInfo = new JavaTypeInfo(List.class, cqlType, null, valueTypeInfo.mainType);
		} else if (type instanceof SetType) {
			JavaTypeInfo valueTypeInfo = getJavaTypeForCassandraType(((SetType) type).getElementType());
			typeInfo = new JavaTypeInfo(Set.class, cqlType, null, valueTypeInfo.mainType);
		} else if (type instanceof MapType) {
			JavaTypeInfo valueTypeInfo = getJavaTypeForCassandraType(((MapType) type).getValueType());
			JavaTypeInfo keyTypeInfo = getJavaTypeForCassandraType(((MapType) type).getKeyType());
			typeInfo = new JavaTypeInfo(Map.class, cqlType, keyTypeInfo.mainType, valueTypeInfo.mainType);
		} else if (type instanceof PrimitiveType) {

			if ("tinyint".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Byte.class, cqlType);
			} else if ("smallint".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Short.class, cqlType);
			} else if ("int".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Integer.class, cqlType);
			} else if ("bigint".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Long.class, cqlType);
			} else if ("counter".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Long.class, cqlType);
			} else if ("decimal".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(BigDecimal.class, cqlType);
			} else if ("double".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Double.class, cqlType);
			} else if ("float".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Float.class, cqlType);
			} else if ("boolean".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(Boolean.class, cqlType);
			} else if ("varchar".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(String.class, cqlType);
			} else if ("text".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(String.class, cqlType);
			} else if ("ascii".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(String.class, cqlType);
			} else if ("uuid".equalsIgnoreCase(cqlType)) {
				typeInfo = new JavaTypeInfo(UUID.class, cqlType);
			}
		}
		return typeInfo;
	}

}
