package com.keytiles.db_migration.model.config;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.keytiles.db_migration.model.BaseEntity;

public class Config extends BaseEntity {

	public static final int DEFAULT_MAX_VARIABLES_CARTESIAN_PRODUCTS_PER_TASK = 16;

	public static Config parseFromYamlFile(String filePath) throws StreamReadException, DatabindException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		JsonNode root = mapper.readTree(new File(filePath));
		return parseFromTree(mapper, root);
	}

	public static Config parseFromYaml(String yamlContent) throws StreamReadException, DatabindException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		JsonNode root = mapper.readTree(yamlContent);
		return parseFromTree(mapper, root);
	}

	/**
	 * Parses config from a YAML/JSON tree: expands {@code tables[]} templates first, then binds the
	 * rest of the document to {@link Config}.
	 */
	static Config parseFromTree(ObjectMapper mapper, JsonNode root) throws DatabindException {
		if (root == null || root.isNull() || !root.isObject()) {
			throw new IllegalArgumentException("config root must be a YAML/JSON object");
		}

		int maxProducts = DEFAULT_MAX_VARIABLES_CARTESIAN_PRODUCTS_PER_TASK;
		JsonNode maxProductsNode = root.get("maxVariablesCartesianProductsPerTask");
		if (maxProductsNode != null && !maxProductsNode.isNull()) {
			maxProducts = maxProductsNode.asInt();
		}

		List<TableMigrationDefinition> expandedTables = TableMigrationTemplateExpander.expandTables(mapper,
				root.get("tables"), maxProducts);

		// Bind Config without raw tables (may contain 'variables' unknown to TableMigrationDefinition)
		ObjectNode rootCopy = ((ObjectNode) root).deepCopy();
		rootCopy.set("tables", mapper.createArrayNode());
		Config config = mapper.convertValue(rootCopy, Config.class);
		config.tables = expandedTables;
		config.maxVariablesCartesianProductsPerTask = maxProducts;
		return config;
	}

	/**
	 * The source DB definition
	 */
	public DBDefinition sourceDB;
	/**
	 * The target DB definition
	 * <p>
	 * Note: It is possible this is pointing to the source DB too! If you want table data migration
	 * within the same DB just between 2 tables... (meaning you use
	 * {@link TableMigrationDefinition#targetTableName})
	 */
	public DBDefinition targetDB;

	public List<TableMigrationDefinition> tables;

	/**
	 * Number of worker threads running migrations - each worker thread deals with one configured table
	 * migration at a time. And they are executed in the order you defined them.
	 *
	 * WARNING! Increase this count >1 only in case ALL your table migrations are independent from any
	 * previous table migrations! As if we do them in parallel then this dependency might be violated!
	 */
	public int threadCount = 1;

	/**
	 * Displays migration status messages (how many rows fetched/migrated) in every this many seconds
	 */
	public long printStatusEveryXSeconds = 60;

	/**
	 * Maximum cartesian product size allowed when expanding one {@code tables[]} entry that declares
	 * {@code variables}. Default {@value #DEFAULT_MAX_VARIABLES_CARTESIAN_PRODUCTS_PER_TASK}. Raise
	 * explicitly if you intentionally need more parallel slices from one template.
	 */
	public int maxVariablesCartesianProductsPerTask = DEFAULT_MAX_VARIABLES_CARTESIAN_PRODUCTS_PER_TASK;

	public Config() {
	}

}
