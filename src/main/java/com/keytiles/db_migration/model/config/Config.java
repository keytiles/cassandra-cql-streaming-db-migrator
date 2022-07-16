package com.keytiles.db_migration.model.config;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.keytiles.db_migration.model.BaseEntity;

public class Config extends BaseEntity {

	public static Config parseFromYamlFile(String filePath) throws StreamReadException, DatabindException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		Config config = mapper.readValue(new File(filePath), Config.class);
		return config;
	}

	public static Config parseFromYaml(String yamlContent) throws StreamReadException, DatabindException, IOException {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		Config config = mapper.readValue(yamlContent, Config.class);
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
	 * Number of worker threads running migrations (it is done on a table basis)
	 */
	public int threadCount = 1;

	/**
	 * Displays migration status messages (how many rows fetched/migrated) in every this many seconds
	 */
	public long printStatusEveryXSeconds = 60;

	public Config() {
	}

}
