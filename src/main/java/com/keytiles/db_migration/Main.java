package com.keytiles.db_migration;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.keytiles.db_migration.model.config.Config;

public class Main {

	private final static String APP_NAME = "java -jar <the jar file>";

	private static Logger LOG = LoggerFactory.getLogger(Main.class);

	public Main() {
	}

	public static void main(String[] args) {

		Options opts = new Options() //
				.addOption(Option.builder("h").desc("displays help").hasArg(false).build()) //
				.addOption(Option.builder("configYaml").required().desc("path to the config .yaml file").hasArg(true)
						.build());

		CommandLineParser cmdParser = new DefaultParser();
		CommandLine cmdLine;
		try {
			cmdLine = cmdParser.parse(opts, args);

			if (cmdLine.hasOption("h")) {
				printUsage(opts, System.out);
				return;
			}

			// let's load the properties file
			String configFilePath = cmdLine.getOptionValue("configYaml");

			Config config = Config.parseFromYamlFile(configFilePath);

			DbMigrator migrator = new DbMigrator(config);
			migrator.migrate();

		} catch (ParseException e) {
			LOG.error("bad usage, error: {}", e.getMessage());
			printUsage(opts, System.out);
		} catch (Exception e) {
			LOG.error("error occured during startup! exiting...", e);
		} finally {
		}
	}

	private static void printUsage(final Options options, final OutputStream out) {
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter usageFormatter = new HelpFormatter();
		usageFormatter.setNewLine("\n");
		usageFormatter.printUsage(writer, 80, APP_NAME, options);
		usageFormatter.printOptions(writer, 80, options, 2, 4);
		writer.close();
	}
}
