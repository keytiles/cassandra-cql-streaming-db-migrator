package com.keytiles.db_migration.model.config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;

/**
 * Expands {@code tables[]} entries that declare a {@code variables} block into concrete
 * {@link TableMigrationDefinition} instances via YAML-text placeholder substitution and cartesian
 * product.
 * <p>
 * Placeholders use {@code ${varName}} syntax. Substitution is single-pass (no recursion).
 */
public final class TableMigrationTemplateExpander {

	private final static Logger LOG = LoggerFactory.getLogger(TableMigrationTemplateExpander.class);

	private final static String VARIABLES_FIELD = "variables";

	/**
	 * Matches {@code ${varName}} where varName is a Java-identifier-like token.
	 */
	private final static Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}");

	private TableMigrationTemplateExpander() {
	}

	/**
	 * Expands all entries in the {@code tables} JSON/YAML array node into concrete definitions.
	 *
	 * @param mapper
	 *            YAML-capable ObjectMapper (same one used for config parse)
	 * @param tablesNode
	 *            the {@code tables} array; may be null/missing
	 * @param maxVariablesCartesianProductsPerTask
	 *            max allowed cartesian size per templated entry
	 * @return flat list of table migration definitions (order preserved)
	 */
	public static List<TableMigrationDefinition> expandTables(ObjectMapper mapper, JsonNode tablesNode,
			int maxVariablesCartesianProductsPerTask) {
		Preconditions.checkArgument(maxVariablesCartesianProductsPerTask >= 1,
				"'maxVariablesCartesianProductsPerTask' must be >= 1 (got %s)", maxVariablesCartesianProductsPerTask);

		List<TableMigrationDefinition> result = new ArrayList<>();
		if (tablesNode == null || tablesNode.isNull() || !tablesNode.isArray()) {
			return result;
		}

		int entryIndex = 0;
		for (JsonNode entryNode : tablesNode) {
			entryIndex++;
			result.addAll(expandOneEntry(mapper, entryNode, entryIndex, maxVariablesCartesianProductsPerTask));
		}
		return result;
	}

	private static List<TableMigrationDefinition> expandOneEntry(ObjectMapper mapper, JsonNode entryNode,
			int entryIndex, int maxProducts) {
		Preconditions.checkArgument(entryNode != null && entryNode.isObject(),
				"tables[%s] must be a YAML/JSON object", entryIndex - 1);

		ObjectNode entryObject = (ObjectNode) entryNode;
		JsonNode variablesNode = entryObject.get(VARIABLES_FIELD);
		if (variablesNode == null || variablesNode.isNull()) {
			TableMigrationDefinition def = treeToTableDef(mapper, entryObject, entryIndex);
			return java.util.Collections.singletonList(def);
		}

		Map<String, List<String>> variables = parseVariables(variablesNode, entryIndex);
		ObjectNode templateNode = entryObject.deepCopy();
		templateNode.remove(VARIABLES_FIELD);

		String templateYaml;
		try {
			templateYaml = mapper.writeValueAsString(templateNode);
		} catch (Exception e) {
			throw new IllegalStateException(
					"tables[" + (entryIndex - 1) + "]: failed to serialize template YAML for variable expansion", e);
		}

		Set<String> placeholders = findPlaceholders(templateYaml);
		Preconditions.checkState(!placeholders.isEmpty(),
				"tables[%s]: 'variables' is set but the template body has no ${...} placeholders — "
						+ "this is likely an operator mistake",
				entryIndex - 1);

		for (String placeholder : placeholders) {
			Preconditions.checkState(variables.containsKey(placeholder),
					"tables[%s]: placeholder '${%s}' has no matching entry under 'variables'", entryIndex - 1,
					placeholder);
		}

		for (Entry<String, List<String>> e : variables.entrySet()) {
			if (!placeholders.contains(e.getKey())) {
				LOG.debug("tables[{}]: variable '{}' is declared but not referenced by any placeholder — ignoring",
						entryIndex - 1, e.getKey());
			}
		}

		// Only referenced variables participate in the cartesian product (declaration order preserved
		// among those that appear in the variables map; product uses variables map key order for all
		// declared keys that are referenced).
		Map<String, List<String>> productVars = new LinkedHashMap<>();
		for (Entry<String, List<String>> e : variables.entrySet()) {
			if (placeholders.contains(e.getKey())) {
				productVars.put(e.getKey(), e.getValue());
			}
		}

		long productSize = 1L;
		for (List<String> values : productVars.values()) {
			productSize *= values.size();
			Preconditions.checkState(productSize <= maxProducts,
					"tables[%s]: variables cartesian product size %s exceeds maxVariablesCartesianProductsPerTask=%s. "
							+ "Although we failed you can extend this by using maxVariablesCartesianProductsPerTask config option",
					entryIndex - 1, productSize, maxProducts);
		}

		List<Map<String, String>> combinations = buildCartesianCombinations(productVars);
		Preconditions.checkState(combinations.size() <= maxProducts,
				"tables[%s]: variables cartesian product size %s exceeds maxVariablesCartesianProductsPerTask=%s. "
						+ "Although we failed you can extend this by using maxVariablesCartesianProductsPerTask config option",
				entryIndex - 1, combinations.size(), maxProducts);

		String tableHint = entryObject.has("tableName") ? entryObject.get("tableName").asText() : ("#" + entryIndex);
		LOG.info("tables[{}] (tableName={}): expanding variables template into {} task(s)", entryIndex - 1, tableHint,
				combinations.size());

		List<TableMigrationDefinition> expanded = new ArrayList<>(combinations.size());
		int variantIndex = 0;
		for (Map<String, String> combo : combinations) {
			String substitutedYaml = substitutePlaceholders(templateYaml, combo);
			TableMigrationDefinition def;
			try {
				def = mapper.readValue(substitutedYaml, TableMigrationDefinition.class);
			} catch (Exception e) {
				throw new IllegalStateException("tables[" + (entryIndex - 1) + "] varVariant_" + variantIndex
						+ ": failed to deserialize expanded YAML into TableMigrationDefinition", e);
			}
			def._varVariantIndex = variantIndex;
			expanded.add(def);
			variantIndex++;
		}
		return expanded;
	}

	private static TableMigrationDefinition treeToTableDef(ObjectMapper mapper, JsonNode entryNode, int entryIndex) {
		try {
			return mapper.treeToValue(entryNode, TableMigrationDefinition.class);
		} catch (Exception e) {
			throw new IllegalStateException(
					"tables[" + (entryIndex - 1) + "]: failed to deserialize TableMigrationDefinition", e);
		}
	}

	private static Map<String, List<String>> parseVariables(JsonNode variablesNode, int entryIndex) {
		Preconditions.checkState(variablesNode.isObject(), "tables[%s].variables must be a map/object", entryIndex - 1);

		Map<String, List<String>> variables = new LinkedHashMap<>();
		Iterator<Entry<String, JsonNode>> fields = variablesNode.fields();
		while (fields.hasNext()) {
			Entry<String, JsonNode> field = fields.next();
			String varName = field.getKey();
			JsonNode valuesNode = field.getValue();
			Preconditions.checkState(valuesNode != null && valuesNode.isArray(),
					"tables[%s].variables.%s must be a list of string pieces", entryIndex - 1, varName);
			Preconditions.checkState(valuesNode.size() > 0,
					"tables[%s].variables.%s is an empty list — would yield zero tasks", entryIndex - 1, varName);

			List<String> values = new ArrayList<>(valuesNode.size());
			for (JsonNode valueNode : valuesNode) {
				Preconditions.checkState(valueNode != null && valueNode.isValueNode(),
						"tables[%s].variables.%s entries must be scalar string pieces", entryIndex - 1, varName);
				values.add(valueNode.asText());
			}
			variables.put(varName, values);
		}

		Preconditions.checkState(!variables.isEmpty(), "tables[%s].variables is empty", entryIndex - 1);
		return variables;
	}

	static Set<String> findPlaceholders(String text) {
		Set<String> names = new LinkedHashSet<>();
		Matcher matcher = PLACEHOLDER_PATTERN.matcher(text);
		while (matcher.find()) {
			names.add(matcher.group(1));
		}
		return names;
	}

	/**
	 * Single-pass replace of each {@code ${name}} with the combination value. Values are inserted as-is
	 * (even if they contain {@code ${...}} text).
	 */
	static String substitutePlaceholders(String templateYaml, Map<String, String> combination) {
		Matcher matcher = PLACEHOLDER_PATTERN.matcher(templateYaml);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			String varName = matcher.group(1);
			String replacement = combination.get(varName);
			Preconditions.checkState(replacement != null, "internal error: missing value for placeholder '${%s}'",
					varName);
			matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * Builds cartesian combinations. Variable key order is map iteration order; the last variable
	 * varies fastest.
	 */
	static List<Map<String, String>> buildCartesianCombinations(Map<String, List<String>> productVars) {
		List<Map<String, String>> combinations = new ArrayList<>();
		List<String> varNames = new ArrayList<>(productVars.keySet());
		if (varNames.isEmpty()) {
			return combinations;
		}

		int[] indexes = new int[varNames.size()];
		boolean done = false;
		while (!done) {
			Map<String, String> combo = new LinkedHashMap<>();
			for (int i = 0; i < varNames.size(); i++) {
				String name = varNames.get(i);
				combo.put(name, productVars.get(name).get(indexes[i]));
			}
			combinations.add(combo);

			// odometer: last index increments first
			done = true;
			for (int i = varNames.size() - 1; i >= 0; i--) {
				indexes[i]++;
				if (indexes[i] < productVars.get(varNames.get(i)).size()) {
					done = false;
					break;
				}
				indexes[i] = 0;
			}
		}
		return combinations;
	}
}
