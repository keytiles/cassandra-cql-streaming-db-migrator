package com.keytiles.db_migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import com.keytiles.db_migration.api.IMigratorPlugin;
import com.keytiles.db_migration.api.IRowSetFilter;
import com.keytiles.db_migration.cassandra.CassandraConnectionAdapter;
import com.keytiles.db_migration.model.BaseEntity;
import com.keytiles.db_migration.model.config.TableDataFilterDefinition;
import com.keytiles.db_migration.model.config.TableMigrationDefinition;
import com.keytiles.db_migration.util.ThreadUtil;
import com.keytiles.db_migration.util.TimeUtil;

/**
 * An instance of this class is responsible for migrating one table
 * <p>
 * The class is a task. It implements {@link Runnable} interface. (Therefore can be assigned easily
 * to a worker thread so we can introduce in-parallel processing...
 *
 * @author AttilaW
 *
 */
public class MigrateTableTask implements Runnable {

	public static enum State {
		initialized, running, finished;
	}

	private final static Logger LOG = LoggerFactory.getLogger(MigrateTableTask.class);

	private final static long HISTOGRAMS_WINDOW_SECONDS = 60;

	private static class RowFilters extends BaseEntity {
		private final int rowsProcessBatchSize;
		private final List<IRowSetFilter> rowSetFilters;

		public RowFilters(int rowsProcessBatchSize, List<IRowSetFilter> rowSetFilters) {
			this.rowsProcessBatchSize = rowsProcessBatchSize;
			this.rowSetFilters = Collections.unmodifiableList(rowSetFilters);
		}

		public int getRowsProcessBatchSize() {
			return rowsProcessBatchSize;
		}

		public List<IRowSetFilter> getRowSetFilters() {
			return rowSetFilters;
		}

	}

	private final String name;

	private final TableMigrationDefinition tableMigrationDefinition;
	private final CassandraConnectionAdapter sourceConnectionAdapter;
	private final CassandraConnectionAdapter targetConnectionAdapter;

	private final ExecutorService writeExecutorService;

	private TableMetadata sourceTableMeta;
	private TableMetadata targetTableMeta;
	private int rowsRead = 0;
	private int rowsPassedFilter = 0;
	private int rowsMigrated = 0;
	private int rowsFailed = 0;

	private State state;
	private long startedTimestamp = -1;
	private long finishedTimestamp = -1;
	/**
	 * If {@link #run()} exited because of an exception then it will be available here.
	 */
	private Throwable failure;
	/**
	 * Process can collect warning messages here
	 */
	protected List<String> warningMessages = new LinkedList<>();

	private long printStatusMessageMillis;
	private long lastStatusPrintTime = -1;
	private RowFilters rowSetFilters;
	private IMigratorPlugin migratorPlugin;

	// some metrics - can be handy to pinpoint migration bottlenecks
	private final MetricRegistry metricRegistry = new MetricRegistry();
	private final Histogram pageFetchMillisHistogram;
	private final Histogram writeBatchTookMillisHistogram;
	private long millisSpentWithReading = 0;
	private long millisSpentWithWriting = 0;

	private final MetricRegistry sourceConnectionMetricRegistry;
	private final MetricRegistry targetConnectionMetricRegistry;

	public MigrateTableTask(TableMigrationDefinition tableMigrationDefinition,
			CassandraConnectionAdapter sourceConnectionAdapter, CassandraConnectionAdapter targetConnectionAdapter,
			@Nullable MetricRegistry sourceConnectionMetricRegistry,
			@Nullable MetricRegistry targetConnectionMetricRegistry) {
		super();
		// let's validate a few things
		Preconditions.checkArgument(sourceConnectionAdapter.getDefaultKeyspaceName() != null,
				"defaultKeyspaceName is not set in sourceConnectionAdapter! Please set it!");
		Preconditions.checkArgument(targetConnectionAdapter.getDefaultKeyspaceName() != null,
				"defaultKeyspaceName is not set in targetConnectionAdapter! Please set it!");

		String name = tableMigrationDefinition.name;
		if (StringUtils.isBlank(name)) {
			name = tableMigrationDefinition.targetTableName == null ? tableMigrationDefinition.tableName
					: tableMigrationDefinition.tableName + "=>" + tableMigrationDefinition.targetTableName;
		}
		this.name = name;

		// writeExecutorService =
		writeExecutorService = Executors.newFixedThreadPool(tableMigrationDefinition.parallelWriteRowCount);

		this.tableMigrationDefinition = tableMigrationDefinition;
		this.sourceConnectionAdapter = sourceConnectionAdapter;
		this.targetConnectionAdapter = targetConnectionAdapter;

		this.sourceConnectionMetricRegistry = sourceConnectionMetricRegistry;
		this.targetConnectionMetricRegistry = targetConnectionMetricRegistry;

		pageFetchMillisHistogram = new Histogram(
				new SlidingTimeWindowReservoir(HISTOGRAMS_WINDOW_SECONDS, TimeUnit.SECONDS));
		metricRegistry.register("pageFetchMillis", pageFetchMillisHistogram);
		writeBatchTookMillisHistogram = new Histogram(
				new SlidingTimeWindowReservoir(HISTOGRAMS_WINDOW_SECONDS, TimeUnit.SECONDS));
		metricRegistry.register("writeBatchTookMillis", writeBatchTookMillisHistogram);

		initialize();
	}

	public void setPrintStatusMessageSeconds(long printStatusMessageSeconds) {
		this.printStatusMessageMillis = printStatusMessageSeconds * 1000;
	}

	private void initialize() {
		LOG.info("{}: initializing and validating...", name);
		try {
			// let's get the source/target table metadata!
			sourceTableMeta = discoverTableSchema(sourceConnectionAdapter, tableMigrationDefinition.tableName);
			String targetTableName = tableMigrationDefinition.getTargetTableName();
			targetTableMeta = discoverTableSchema(targetConnectionAdapter, targetTableName);

			Preconditions.checkState(tableMigrationDefinition.continueOnRowError != null,
					"'continueOnRowError' is not set! Please configure it for TRUE or FALSE explicitly!");

			rowSetFilters = createRowsFilters(tableMigrationDefinition);
			migratorPlugin = createMigratorPlugin(tableMigrationDefinition);

		} catch (Throwable t) {
			LOG.error("{}: failed with exception: " + t, name, t);
			failure = t;
			throw t;
		}
		state = State.initialized;
		LOG.info("{}: init done", name);
	}

	private RowFilters createRowsFilters(TableMigrationDefinition tableMigrationDefinition) {
		List<IRowSetFilter> filters = new ArrayList<>();
		// we will do a minimum search over this field - let's start from the page size
		int rowsProcessBatchSize = tableMigrationDefinition.pageSize;
		if (tableMigrationDefinition.dataFilterDefinitions != null) {
			for (TableDataFilterDefinition filterDefinition : tableMigrationDefinition.dataFilterDefinitions) {
				IRowSetFilter filterInstance = filterDefinition.getPluginInstance(sourceTableMeta, targetTableMeta,
						tableMigrationDefinition, sourceConnectionAdapter.getSession(),
						targetConnectionAdapter.getSession());
				filters.add(filterInstance);
				LOG.info("row set filter is created! from {}", filterDefinition);
				// ... and the batch size is...
				if (filterDefinition.maxRowsBatchSize != null
						&& filterDefinition.maxRowsBatchSize < rowsProcessBatchSize) {
					rowsProcessBatchSize = filterDefinition.maxRowsBatchSize;
				}
			}
		}

		return new RowFilters(rowsProcessBatchSize, filters);
	}

	private IMigratorPlugin createMigratorPlugin(TableMigrationDefinition tableMigrationDefinition) {
		Preconditions.checkState(tableMigrationDefinition.migratorPluginDefinition != null,
				"tableMigrationDefinition.migratorPluginDefinition can not be NULL! Problem opccued in %s",
				tableMigrationDefinition);
		IMigratorPlugin pluginInstance = tableMigrationDefinition.migratorPluginDefinition.getPluginInstance(
				sourceTableMeta, targetTableMeta, tableMigrationDefinition, sourceConnectionAdapter.getSession(),
				targetConnectionAdapter.getSession());

		LOG.info("MigratorPlugin is created! from {}", tableMigrationDefinition.migratorPluginDefinition);

		return pluginInstance;
	}

	public State getState() {
		return state;
	}

	public boolean isFinished() {
		return state == State.finished;
	}

	public int getRowsRead() {
		return rowsRead;
	}

	public int getRowsPassedFilter() {
		return rowsPassedFilter;
	}

	public int getRowsMigrated() {
		return rowsMigrated;
	}

	public int getRowsFailed() {
		return rowsFailed;
	}

	public Throwable getFailure() {
		Preconditions.checkState(state == State.finished,
				"invoking getFailure() does not make sense until the task is not finished");
		return failure;
	}

	public List<String> getWarningMessages() {
		List<String> migratorWarns = migratorPlugin.getWarningMessages();
		List<String> warns = new ArrayList<>(warningMessages.size() + migratorWarns.size());
		warns.addAll(warns);
		warns.addAll(migratorWarns);
		return warns;
	}

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public long getMillisSpentWithReading() {
		return millisSpentWithReading;
	}

	public long getMillisSpentWithWriting() {
		return millisSpentWithWriting;
	}

	public TableMigrationDefinition getTableDefinition() {
		return tableMigrationDefinition;
	}

	public long getStartedTimestamp() {
		return startedTimestamp;
	}

	public long getFinishedTimestamp() {
		return finishedTimestamp;
	}

	@Override
	public void run() {
		Preconditions.checkState(state == State.initialized, "Task '%s' was already performed - can not re-run!", name);

		LOG.info("{}: starting...", name);
		state = State.running;
		startedTimestamp = System.currentTimeMillis();
		lastStatusPrintTime = startedTimestamp;

		millisSpentWithReading = 0;
		millisSpentWithWriting = 0;

		try {

			// the read query
			SimpleStatement query = migratorPlugin.getReadQuery();

			int rowsProcessBatchSize = rowSetFilters.getRowsProcessBatchSize();

			// we will collect up read rows into an array
			List<Row> rowsFetched = new ArrayList<>(rowsProcessBatchSize);

			ResultSet result = sourceConnectionAdapter.getSession().execute(query);
			Iterator<Row> rowsIterator = result.iterator();
			long pageFetchStarted = System.currentTimeMillis();
			long millisSpentInWritesSinceLastPageFetch = 0;
			long millisSpentInWaitingWritesSinceLastPageFetch = 0;
			while (rowsIterator.hasNext() && !isMaxWriteRowCountReached()) {

				Row row = rowsIterator.next();
				rowsFetched.add(row);
				rowsRead++;

				boolean pageSizeReached = rowsRead % tableMigrationDefinition.pageSize == 0;
				if (pageSizeReached) {
					long pageFetchTookMillis = System.currentTimeMillis() - pageFetchStarted
							- millisSpentInWritesSinceLastPageFetch - millisSpentInWaitingWritesSinceLastPageFetch;
					pageFetchMillisHistogram.update(pageFetchTookMillis);
					millisSpentWithReading += pageFetchTookMillis;
					millisSpentInWritesSinceLastPageFetch = 0;
					millisSpentInWaitingWritesSinceLastPageFetch = 0;
					pageFetchStarted = System.currentTimeMillis();

					if (tableMigrationDefinition.pauseMillisBetweenPages > 0) {
						LOG.info("{}: page is exhausted - taking a break... ({} msec)", name,
								tableMigrationDefinition.pauseMillisBetweenPages);
						ThreadUtil.waitMillis(tableMigrationDefinition.pauseMillisBetweenPages);
						millisSpentInWaitingWritesSinceLastPageFetch += tableMigrationDefinition.pauseMillisBetweenPages;
					}
				}

				// is the num of collected rows reached the batch size?
				if (rowsFetched.size() == rowsProcessBatchSize) {
					long writeStartedAt = System.currentTimeMillis();
					migrateFetchedRows(rowsFetched);
					// and let's start over
					rowsFetched.clear();
					millisSpentInWritesSinceLastPageFetch += System.currentTimeMillis() - writeStartedAt;
				}

				if (System.currentTimeMillis() >= lastStatusPrintTime + printStatusMessageMillis) {
					printStatusLog();
				}
			}
			// we might have still rows to process...
			migrateFetchedRows(rowsFetched);

			if (isMaxWriteRowCountReached()) {
				LOG.info("{}: maxWriteRowCount of {} reached - aborting...", name,
						tableMigrationDefinition.maxWriteRowCount);
			}

		} catch (Throwable t) {
			LOG.error("{}: failed with exception: ", name, t);
			failure = t;
		} finally {
			LOG.info("{}: finished", name);
			printStatusLog();

			state = State.finished;
			finishedTimestamp = System.currentTimeMillis();
		}
	}

	private int _prevRowsRead = 0;
	private int _prevRowsPassedFilter = 0;
	private int _prevRowsMigrated = 0;
	private long _prevMillisSpentWithReading = 0;
	private long _prevMillisSpentWithWriting = 0;

	private void printStatusLog() {

		int deltaRowsRead = rowsRead - _prevRowsRead;
		int deltaRowsPassedFilter = rowsPassedFilter - _prevRowsPassedFilter;
		int deltaRowsMigrated = rowsMigrated - _prevRowsMigrated;
		long deltaMillisSpentWithReading = millisSpentWithReading - _prevMillisSpentWithReading;
		long deltaMillisSpentWithWriting = millisSpentWithWriting - _prevMillisSpentWithWriting;
		_prevRowsRead = rowsRead;
		_prevRowsPassedFilter = rowsPassedFilter;
		_prevRowsMigrated = rowsMigrated;
		_prevMillisSpentWithReading = millisSpentWithReading;
		_prevMillisSpentWithWriting = millisSpentWithWriting;

		if (tableMigrationDefinition.simulateOnly) {
			LOG.info(
					"{}: rows read: {} (+{}), rows passed filtering: {} (+{}), rows migrated (simulation mode!): {} (+{}), rows failed: {}",
					name, rowsRead, deltaRowsRead, rowsPassedFilter, deltaRowsPassedFilter, rowsMigrated,
					deltaRowsMigrated, rowsFailed);
		} else {
			LOG.info(
					"{}: rows read: {} (+{}), rows passed filtering: {} (+{}), rows migrated: {} (+{}), rows failed: {}",
					name, rowsRead, deltaRowsRead, rowsPassedFilter, deltaRowsPassedFilter, rowsMigrated,
					deltaRowsMigrated, rowsFailed);
		}

		float pageFetchMillisMean = Math.round(pageFetchMillisHistogram.getSnapshot().getMean() * 100) / 100;
		float writeBatchTookMillisMean = Math.round(writeBatchTookMillisHistogram.getSnapshot().getMean() * 100) / 100;
		LOG.info(
				"{}: started {} ago | time spent in reading/writing: {} (+{}) / {} (+{}) | pageFetchMean: {} msec (pageSize {}), writeBatchTookMean: {} msec (processBatchSize {}) - with sliding window {} secs",
				name, TimeUtil.millisToHumanReadableString(System.currentTimeMillis() - startedTimestamp),
				TimeUtil.millisToHumanReadableString(millisSpentWithReading),
				TimeUtil.millisToHumanReadableString(deltaMillisSpentWithReading),
				TimeUtil.millisToHumanReadableString(millisSpentWithWriting),
				TimeUtil.millisToHumanReadableString(deltaMillisSpentWithWriting), pageFetchMillisMean,
				tableMigrationDefinition.pageSize, writeBatchTookMillisMean, rowSetFilters.rowsProcessBatchSize,
				HISTOGRAMS_WINDOW_SECONDS);

		lastStatusPrintTime = System.currentTimeMillis();
	}

	private TableMetadata discoverTableSchema(CassandraConnectionAdapter connectionAdapter, String tableName) {
		// let's get the table metadata!
		Optional<KeyspaceMetadata> ksMetadata = connectionAdapter.getSession().getMetadata()
				.getKeyspace(connectionAdapter.getDefaultKeyspaceName());
		Preconditions.checkState(ksMetadata.isPresent(),
				"Querying metadata of keyspace '%s' using '%s' connection was not successful it looks... Does it exist???",
				connectionAdapter.getDefaultKeyspaceName(), connectionAdapter.getName());
		Preconditions.checkState(ksMetadata.get().getTable(tableName).isPresent(),
				"Querying metadata of table '%s' in keyspace '%s' using '%s' connection was not successful it looks... Does it exist???",
				tableName, connectionAdapter.getDefaultKeyspaceName(), connectionAdapter.getName());

		// OK we have metadata!
		return ksMetadata.get().getTable(tableName).get();
	}

	private static class RowMigrationTask {

	}

	/**
	 * The soul of migration. This method gets a set of Rows. It executes the filter on them and migrate
	 * what is passed the filter
	 *
	 * @param fetchedRows
	 */
	private void migrateFetchedRows(List<Row> fetchedRows) {

		long startedAt = System.currentTimeMillis();

		// first let's filter what we read (if there are filters)
		List<Row> filteredRows = fetchedRows;
		for (IRowSetFilter filter : rowSetFilters.getRowSetFilters()) {
			filteredRows = filter.filterRowSet(filteredRows);
		}
		rowsPassedFilter += filteredRows.size();

		boolean abort = false;
		Iterator<Row> rowsIterator = filteredRows.iterator();
		while (rowsIterator.hasNext() && !abort) {
			Row row = rowsIterator.next();

			if (isMaxWriteRowCountReached()) {
				// let's exit - limit reached
				abort = true;
			}

			if (!abort) {
				try {
					if (migratorPlugin.migrateRow(row)) {
						rowsMigrated++;
					}
				} catch (Exception e) {
					rowsFailed++;
					String msg = "row migration failed with exception: " + e;
					if (tableMigrationDefinition.continueOnRowError) {
						// this just a warning then
						LOG.warn(msg);
					} else {
						// hard error
						throw new IllegalStateException(msg, e);
					}
				}
			}
		}

		long writeTookMillis = System.currentTimeMillis() - startedAt;
		writeBatchTookMillisHistogram.update(writeTookMillis);
		millisSpentWithWriting += writeTookMillis;
	}

	private boolean isMaxWriteRowCountReached() {
		return (tableMigrationDefinition.maxWriteRowCount > 0
				&& rowsMigrated >= tableMigrationDefinition.maxWriteRowCount);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MigrateTableTask [name=").append(name).append(", state=").append(state).append("]");
		return builder.toString();
	}

}
