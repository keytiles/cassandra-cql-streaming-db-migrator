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
			// sleep() clears the interrupt flag — restore it so callers / shutdown can react
			Thread.currentThread().interrupt();
			LOG.debug("sleep interrupted after requesting {} msec wait", millis);
		}
	}

}
