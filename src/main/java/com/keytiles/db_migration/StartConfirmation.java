package com.keytiles.db_migration;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asks the operator to confirm before migration work is scheduled. Supports interactive Y/N and
 * unix-style assume-yes ({@code -y}) for unattended runs such as {@code nohup}.
 */
public final class StartConfirmation {

	private final static Logger LOG = LoggerFactory.getLogger(StartConfirmation.class);

	private StartConfirmation() {
	}

	/**
	 * @param assumeYes
	 *            if true (CLI {@code -y}/{@code --yes}), skip the prompt and proceed
	 * @return true if migration should start; false if the operator declined
	 * @throws IllegalStateException
	 *             if the session is non-interactive and {@code assumeYes} is false
	 */
	public static boolean confirmStart(boolean assumeYes) {
		if (assumeYes) {
			LOG.info("start confirmation skipped (-y/--yes): proceeding with migration");
			return true;
		}

		if (!isInteractive()) {
			throw new IllegalStateException(
					"Non-interactive session (e.g. nohup) detected and -y/--yes was not passed. "
							+ "Re-run with -y to confirm the task list automatically and start migration.");
		}

		LOG.info("Type Y to start migration, or N to abort:");
		String answer = readLineFromConsoleOrStdin();
		if (answer == null) {
			throw new IllegalStateException("No answer received for start confirmation; aborting.");
		}
		String normalized = answer.trim();
		if ("Y".equalsIgnoreCase(normalized) || "YES".equalsIgnoreCase(normalized)) {
			LOG.info("operator confirmed start (answer={})", normalized);
			return true;
		}
		LOG.info("operator declined start (answer={})", normalized);
		return false;
	}

	static boolean isInteractive() {
		Console console = System.console();
		return console != null;
	}

	private static String readLineFromConsoleOrStdin() {
		Console console = System.console();
		if (console != null) {
			return console.readLine();
		}
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
			return reader.readLine();
		} catch (IOException e) {
			throw new IllegalStateException("failed to read start confirmation from stdin", e);
		}
	}
}
