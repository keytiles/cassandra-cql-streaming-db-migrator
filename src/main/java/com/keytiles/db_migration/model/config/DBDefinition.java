package com.keytiles.db_migration.model.config;

import com.keytiles.db_migration.model.BaseEntity;

public class DBDefinition extends BaseEntity {

	/**
	 * Comma separated list of contact nodes like "host1:9042, host2:9042"
	 */
	public String contactNodes;

	public String contactNodesDatacenterName;

	public String keyspaceName;

}
