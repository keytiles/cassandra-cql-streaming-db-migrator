package com.keytiles.db_migration.model.config;

import com.keytiles.db_migration.model.BaseEntity;

public class RetryStrategy extends BaseEntity {

	public int retryCount;
	public long pauseMillisBetweenRetries;
	public long exponentialPauseMultiplier;

	public RetryStrategy() {
	}

	public RetryStrategy(int retryCount, long pauseMillisBetweenRetries, long exponentialPauseMultiplier) {
		this.retryCount = retryCount;
		this.pauseMillisBetweenRetries = pauseMillisBetweenRetries;
		this.exponentialPauseMultiplier = exponentialPauseMultiplier;
	}

}
