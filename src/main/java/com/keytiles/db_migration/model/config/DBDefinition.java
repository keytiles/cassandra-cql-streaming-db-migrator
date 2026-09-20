package com.keytiles.db_migration.model.config;

import com.keytiles.db_migration.model.BaseEntity;

public class DBDefinition extends BaseEntity {

	/**
	 * Comma separated list of contact nodes like "host1:9042, host2:9042"
	 */
	public String contactNodes;

	public String contactNodesDatacenterName;

	public String keyspaceName;

	/**
	 * The generic timeout value. If not specified at all the default (10000) value is used.
	 *
	 * BUT IMPORTANT! This just controls the client timeout! It is NOT involved necessarily to server
	 * side between coordinator -> replicas communication! Keep that in mind! When you do a migration
	 * and you run into read timeouts it is better strategy to consider work with smaller `pageSize`s to
	 * decrease stress on server side!
	 */
	public Integer requestTimeoutMillis;
	/**
	 * If NULL/not set then `requestTimeoutMillis` (with default 10000) is used here. If set then the
	 * set value.
	 *
	 * BUT IMPORTANT! This just controls the client timeout! It is NOT involved necessarily to server
	 * side between coordinator -> replicas communication! Keep that in mind! When you do a migration
	 * and you run into read timeouts it is better strategy to consider work with smaller `pageSize`s to
	 * decrease stress on server side!
	 */
	public Integer firstPageTimeoutMillis;
	/**
	 * If NULL/not set then `requestTimeoutMillis` (with default 10000) is used here. If set then the
	 * set value.
	 *
	 * BUT IMPORTANT! This just controls the client timeout! It is NOT involved necessarily to server
	 * side between coordinator -> replicas communication! Keep that in mind! When you do a migration
	 * and you run into read timeouts it is better strategy to consider work with smaller `pageSize`s to
	 * decrease stress on server side!
	 */
	public Integer followingPagesTimeoutMillis;

}
