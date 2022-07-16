package com.keytiles.db_migration.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtil {

	private final static Logger LOG = LoggerFactory.getLogger(ThreadUtil.class);

	private ThreadUtil() {
	}

	public static void waitMillis(long millis) {
		if (millis <= 0) {
			return;
		}
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			LOG.warn("exception while thread sleep...", e);
		}
	}

}
