package com.keytiles.db_migration;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.keytiles.db_migration.cassandra.CassandraConnectionAdapter;
import com.keytiles.db_migration.model.config.Config;
import com.keytiles.db_migration.model.config.DBDefinition;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;
import com.keytiles.db_migration.util.ThreadUtil;
import com.keytiles.db_migration.util.TimeUtil;

public class DbMigrator {

	private final static Logger LOG = LoggerFactory.getLogger(DbMigrator.class);

	private final Config config;

	private CassandraConnectionAdapter sourceConnectionAdapter;
	private CassandraConnectionAdapter targetConnectionAdapter;

	private final ScheduledExecutorService executorService;
	private final Set<MigrateTableTask> migrateTasks;

	public DbMigrator(Config config) {
		this.config = config;

		executorService = Executors.newScheduledThreadPool(config.threadCount);
		migrateTasks = new LinkedHashSet<>();
	}

	public void migrate() {
		LOG.info("starting migration...");
		LOG.debug("config to be used: {}", config);

		try {

			MetricRegistry sourceConnectionMetricRegistry = new MetricRegistry();
			MetricRegistry targetConnectionMetricRegistry = new MetricRegistry();
			sourceConnectionAdapter = openConnection("sourceDB", config.sourceDB, sourceConnectionMetricRegistry);
			targetConnectionAdapter = openConnection("targetDB", config.targetDB, targetConnectionMetricRegistry);

			// let's prepare tasks - one / tables
			LOG.info("preparing table migration tasks...");
			Set<MigrateTableTask> failedInitTasks = new HashSet<>();
			for (TableMigrationDefinition tableDef : config.tables) {
				MigrateTableTask task = null;
				try {
					task = new MigrateTableTask(tableDef, sourceConnectionAdapter, targetConnectionAdapter,
							sourceConnectionMetricRegistry, targetConnectionMetricRegistry);
					task.setPrintStatusMessageSeconds(config.printStatusEveryXSeconds);
					migrateTasks.add(task);
				} catch (Exception e) {
					failedInitTasks.add(task);
				}
			}

			// Do we have failed ones?
			Preconditions.checkState(failedInitTasks.isEmpty(),
					"Exiting migration as %s table migration task(s) have indicated issues...", failedInitTasks.size());

			LOG.info("scheduling table migration tasks... parallel processing is set to {} threads",
					config.threadCount);
			for (MigrateTableTask task : migrateTasks) {
				executorService.schedule(task, 0, TimeUnit.MILLISECONDS);
			}

			LOG.info("waiting for all migration tasks to complete...");
			while (!isAllTasksComplete()) {
				// we wait...
				ThreadUtil.waitMillis(1000);
			}

			executorService.shutdown();

			// let's dump some final statistics
			LOG.info("All tasks finished! Stats:");
			for (MigrateTableTask task : migrateTasks) {
				long timeTook = task.getFinishedTimestamp() - task.getStartedTimestamp();
				String resultMsg = task.getFailure() == null ? "successful"
						: "failed! Error was: " + task.getFailure().getMessage();
				List<String> taskWarnings = task.getWarningMessages();
				String warnPrefix = "\n      - WARN: ";
				String warningsMsg = taskWarnings.isEmpty() ? "no warnings"
						: warnPrefix + Joiner.on(warnPrefix).join(taskWarnings);

				LOG.info(
						"task for table '{}': {}\n   - stats: took {}, rowsRead: {}, rowsPassedFiltering: {}, rowsMigrated (written to target): {}, rowsFailed: {}\n   - warnings: {}",
						task.getTableDefinition().tableName, resultMsg, TimeUtil.millisToHumanReadableString(timeTook),
						task.getRowsRead(), task.getRowsPassedFilter(), task.getRowsMigrated(), task.getRowsFailed(),
						warningsMsg);
			}

		} catch (Throwable t) {

			LOG.error("exception occured during process! exception was:", t);

		} finally {
			IOUtils.closeQuietly(sourceConnectionAdapter, null);
			IOUtils.closeQuietly(targetConnectionAdapter, null);
		}

		LOG.info("migration DONE!");
	}

	private CassandraConnectionAdapter openConnection(String name, DBDefinition dbDef,
			@Nullable MetricRegistry connectionMetricRegistry) {
		LOG.info("opening connection for: {} ...", name);
		CassandraConnectionAdapter connAdapter = new CassandraConnectionAdapter(name, dbDef.contactNodes,
				dbDef.contactNodesDatacenterName, connectionMetricRegistry);
		connAdapter.setDefaultKeyspaceName(dbDef.keyspaceName);
		connAdapter.connect();
		LOG.info("connection established!", name);
		return connAdapter;
	}

	private boolean isAllTasksComplete() {
		for (MigrateTableTask task : migrateTasks) {
			if (!task.isFinished()) {
				return false;
			}
		}
		return true;
	}

}
