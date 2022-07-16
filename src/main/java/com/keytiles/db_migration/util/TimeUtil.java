package com.keytiles.db_migration.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class TimeUtil {

	private TimeUtil() {
	}

	public static String millisToHumanReadableString(long millis) {
		if (millis < 1000) {
			return millis + "ms";
		}
		return secondsToHumanReadableString((int) (millis / 1000));
	}

	public static String secondsToHumanReadableString(int seconds) {
		int mins = seconds / 60;
		seconds = seconds - mins * 60;

		int hours = mins / 60;
		mins = mins - hours * 60;

		int days = hours / 24;
		hours = hours - days * 24;

		// now put together
		List<String> parts = new ArrayList<>(4);
		if (days > 0) {
			parts.add(days + "d");
		}
		if (hours > 0) {
			parts.add(hours + "h");
		}
		if (mins > 0) {
			parts.add(mins + "m");
		}
		if (seconds > 0 || parts.isEmpty()) {
			parts.add(seconds + "s");
		}
		return Joiner.on(":").join(parts);
	}

	public static String utcMillisToFormattedTimestamp(long ts) {
		return Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDateTime()
				.format(DateTimeFormatter.ISO_DATE_TIME);
	}

}
