package com.keytiles.db_migration.model.config;

import com.keytiles.db_migration.model.BaseEntity;

/**
 * After source page-fetch retries are exhausted, optionally reduce the effective page size and try
 * again with the same paging state (smaller pages are often easier under load).
 */
public class PageSizeReduceStrategy extends BaseEntity {

	/**
	 * Integer divisor applied to the current effective page size (e.g. 2 means half).
	 * Must be &gt;= 2.
	 */
	public int reducePageSizeFactor;

	/**
	 * How many times the reduce factor may be applied during the whole table migration.
	 * Set to 0 to disable page-size reduction.
	 */
	public int maxIteration;

	public PageSizeReduceStrategy() {
	}

	public PageSizeReduceStrategy(int reducePageSizeFactor, int maxIteration) {
		this.reducePageSizeFactor = reducePageSizeFactor;
		this.maxIteration = maxIteration;
	}

}
