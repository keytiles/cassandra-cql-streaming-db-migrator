package com.keytiles.db_migration.model.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Feature: table migration task template expansion ({@code variables} + {@code ${placeholders}}).
 */
public class TableMigrationTemplateExpanderTest {

	// ############################################################################################
	// Feature: expand tables with variables DSL
	// ############################################################################################

	@Test
	public void expandTables_noVariables_keepsSingleEntry() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "sourceDB:\n" //
				+ "  keyspaceName: ks\n" //
				+ "  contactNodes: localhost:9042\n" //
				+ "  contactNodesDatacenterName: dc1\n" //
				+ "targetDB:\n" //
				+ "  keyspaceName: ks\n" //
				+ "  contactNodes: localhost:9042\n" //
				+ "  contactNodesDatacenterName: dc1\n" //
				+ "tables:\n" //
				+ "  - tableName: t1\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"a = 1\"\n";

		// ---- WHEN
		Config config = Config.parseFromYaml(yaml);

		// ---- THEN
		assertEquals(1, config.tables.size());
		assertEquals("t1", config.tables.get(0).tableName);
		assertEquals("a = 1", config.tables.get(0).whereClause);
		assertNull(config.tables.get(0)._varVariantIndex);
		assertEquals(Config.DEFAULT_MAX_VARIABLES_CARTESIAN_PRODUCTS_PER_TASK,
				config.maxVariablesCartesianProductsPerTask);
	}

	@Test
	public void expandTables_twoVars_cartesianProductAndWhereClause() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"c in (${containers}) and d in (${dists})\"\n" //
				+ "    variables:\n" //
				+ "      containers:\n" //
				+ "        - \"'a','b'\"\n" //
				+ "        - \"'c','d'\"\n" //
				+ "      dists:\n" //
				+ "        - \"0,1\"\n" //
				+ "        - \"2,3\"\n";

		// ---- WHEN
		Config config = Config.parseFromYaml(minimalDbPreamble() + yaml);

		// ---- THEN
		assertEquals(4, config.tables.size());
		assertEquals("c in ('a','b') and d in (0,1)", config.tables.get(0).whereClause);
		assertEquals(Integer.valueOf(0), config.tables.get(0)._varVariantIndex);
		assertEquals("c in ('a','b') and d in (2,3)", config.tables.get(1).whereClause);
		assertEquals(Integer.valueOf(1), config.tables.get(1)._varVariantIndex);
		assertEquals("c in ('c','d') and d in (0,1)", config.tables.get(2).whereClause);
		assertEquals(Integer.valueOf(2), config.tables.get(2)._varVariantIndex);
		assertEquals("c in ('c','d') and d in (2,3)", config.tables.get(3).whereClause);
		assertEquals(Integer.valueOf(3), config.tables.get(3)._varVariantIndex);
	}

	@Test
	public void expandTables_mvelRuleInFilterOptions_isSubstituted() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"x = 1\"\n" //
				+ "    dataFilterDefinitions:\n" //
				+ "      - filterClass: com.keytiles.db_migration.implementation.FieldValueFilter\n" //
				+ "        filterOptions:\n" //
				+ "          mvelRule: \"row.time_frame_id >= ${tfFrom} && row.time_frame_id < ${tfTo}\"\n" //
				+ "    variables:\n" //
				+ "      tfFrom:\n" //
				+ "        - \"10\"\n" //
				+ "      tfTo:\n" //
				+ "        - \"20\"\n";

		// ---- WHEN
		Config config = Config.parseFromYaml(minimalDbPreamble() + yaml);

		// ---- THEN
		assertEquals(1, config.tables.size());
		assertEquals(Integer.valueOf(0), config.tables.get(0)._varVariantIndex);
		Object mvel = config.tables.get(0).dataFilterDefinitions.get(0).filterOptions.get("mvelRule");
		assertEquals("row.time_frame_id >= 10 && row.time_frame_id < 20", mvel);
	}

	@Test
	public void expandTables_variablesWithoutPlaceholders_fails() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"x = 1\"\n" //
				+ "    variables:\n" //
				+ "      unused:\n" //
				+ "        - \"a\"\n";

		// ---- WHEN / THEN
		try {
			Config.parseFromYaml(minimalDbPreamble() + yaml);
			fail("expected IllegalStateException");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("no ${...} placeholders"));
		}
	}

	@Test
	public void expandTables_emptyVariableList_fails() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"c in (${containers})\"\n" //
				+ "    variables:\n" //
				+ "      containers: []\n";

		// ---- WHEN / THEN
		try {
			Config.parseFromYaml(minimalDbPreamble() + yaml);
			fail("expected IllegalStateException");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("empty list"));
		}
	}

	@Test
	public void expandTables_missingPlaceholderVariable_fails() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"c in (${containers})\"\n" //
				+ "    variables:\n" //
				+ "      other:\n" //
				+ "        - \"a\"\n";

		// ---- WHEN / THEN
		try {
			Config.parseFromYaml(minimalDbPreamble() + yaml);
			fail("expected IllegalStateException");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("${containers}"));
		}
	}

	@Test
	public void expandTables_productExceedsCap_failsWithConfigHint() throws Exception {
		// ---- GIVEN
		String yaml = "" //
				+ "maxVariablesCartesianProductsPerTask: 2\n" //
				+ "tables:\n" //
				+ "  - tableName: counters\n" //
				+ "    continueOnRowError: true\n" //
				+ "    whereClause: \"c in (${a}) and d in (${b})\"\n" //
				+ "    variables:\n" //
				+ "      a:\n" //
				+ "        - \"1\"\n" //
				+ "        - \"2\"\n" //
				+ "      b:\n" //
				+ "        - \"3\"\n" //
				+ "        - \"4\"\n";

		// ---- WHEN / THEN
		try {
			Config.parseFromYaml(minimalDbPreamble() + yaml);
			fail("expected IllegalStateException");
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("maxVariablesCartesianProductsPerTask"));
			assertTrue(e.getMessage().contains("Although we failed you can extend this"));
		}
	}

	@Test
	public void startConfirmation_assumeYes_returnsTrue() {
		// ---- GIVEN / WHEN / THEN
		assertTrue(com.keytiles.db_migration.StartConfirmation.confirmStart(true));
	}

	private static String minimalDbPreamble() {
		return "" //
				+ "sourceDB:\n" //
				+ "  keyspaceName: ks\n" //
				+ "  contactNodes: localhost:9042\n" //
				+ "  contactNodesDatacenterName: dc1\n" //
				+ "targetDB:\n" //
				+ "  keyspaceName: ks\n" //
				+ "  contactNodes: localhost:9042\n" //
				+ "  contactNodesDatacenterName: dc1\n";
	}
}
