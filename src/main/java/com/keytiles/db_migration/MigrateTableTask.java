package com.keytiles.db_migration;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.google.common.base.Preconditions;
import com.keytiles.db_migration.api.IMigratorPlugin;
import com.keytiles.db_migration.api.IRowSetFilter;
import com.keytiles.db_migration.cassandra.CassandraConnectionAdapter;
import com.keytiles.db_migration.model.BaseEntity;
import com.keytiles.db_migration.model.config.PageSizeReduceStrategy;
import com.keytiles.db_migration.model.config.RetryStrategy;
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

	/**
	 * Filters for this table migration, plus optional chunk size for filter/write batches.
	 * {@code maxRowsBatchSize} is the minimum of configured filter {@code maxRowsBatchSize} values, or
	 * {@code null} when no filter limits chunking (whole driver page is processed at once).
	 */
	private static class RowFilters extends BaseEntity {
		private final List<IRowSetFilter> rowSetFilters;
		private final Integer maxRowsBatchSize;

		public RowFilters(List<IRowSetFilter> rowSetFilters, Integer maxRowsBatchSize) {
			this.rowSetFilters = Collections.unmodifiableList(rowSetFilters);
			this.maxRowsBatchSize = maxRowsBatchSize;
		}

		public List<IRowSetFilter> getRowSetFilters() {
			return rowSetFilters;
		}

		public Integer getMaxRowsBatchSize() {
			return maxRowsBatchSize;
		}
	}

	private final String name;
	private final int index;

	private final TableMigrationDefinition tableMigrationDefinition;
	private final CassandraConnectionAdapter sourceConnectionAdapter;
	private final CassandraConnectionAdapter targetConnectionAdapter;

	private ExecutorService writeExecutorService;

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

	private final RetryStrategy writeRetryStrategy;
	private final RetryStrategy readRetryStrategy;
	private final PageSizeReduceStrategy pageSizeReduceStrategy;

	/**
	 * Effective page size for source reads — starts as configured {@code pageSize}, may shrink via
	 * {@link #pageSizeReduceStrategy} and then stays reduced for later pages.
	 */
	private int effectivePageSize;
	/** How many times page size was reduced so far in this task (capped by maxIteration). */
	private int pageSizeReduceIterationsApplied;

	public MigrateTableTask(int index, TableMigrationDefinition tableMigrationDefinition,
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
		Preconditions.checkArgument(StringUtils.isNotBlank(name),
				"tableMigrationDefinition.name must be set by DbMigrator before creating MigrateTableTask");
		this.name = name;
		this.index = index;

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

		writeRetryStrategy = tableMigrationDefinition.writeRetryStrategy;
		validateRetryStrategy(writeRetryStrategy, "writeRetryStrategy");
		readRetryStrategy = tableMigrationDefinition.readRetryStrategy;
		validateRetryStrategy(readRetryStrategy, "readRetryStrategy");
		pageSizeReduceStrategy = tableMigrationDefinition.pageSizeReduceStrategy;
		validatePageSizeReduceStrategy(pageSizeReduceStrategy);

		initialize();
	}

	private static void validateRetryStrategy(RetryStrategy retryStrategy, String fieldName) {
		if (retryStrategy == null) {
			return;
		}
		Preconditions.checkArgument(retryStrategy.retryCount >= 0, "Invalid '%s.retryCount' value - it must be >= 0",
				fieldName);
		Preconditions.checkArgument(retryStrategy.pauseMillisBetweenRetries >= 0,
				"Invalid '%s.pauseMillisBetweenRetries' value - it must be >= 0", fieldName);
		Preconditions.checkArgument(retryStrategy.exponentialPauseMultiplier >= 1,
				"Invalid '%s.exponentialPauseMultiplier' value - it must be >= 1", fieldName);
	}

	private static void validatePageSizeReduceStrategy(PageSizeReduceStrategy strategy) {
		if (strategy == null) {
			return;
		}
		Preconditions.checkArgument(strategy.reducePageSizeFactor >= 2,
				"Invalid 'pageSizeReduceStrategy.reducePageSizeFactor' value - it must be >= 2");
		Preconditions.checkArgument(strategy.maxIteration >= 0,
				"Invalid 'pageSizeReduceStrategy.maxIteration' value - it must be >= 0");
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
		Integer maxRowsBatchSize = null;
		if (tableMigrationDefinition.dataFilterDefinitions != null) {
			for (TableDataFilterDefinition filterDefinition : tableMigrationDefinition.dataFilterDefinitions) {
				IRowSetFilter filterInstance = filterDefinition.getPluginInstance(sourceTableMeta, targetTableMeta,
						tableMigrationDefinition, sourceConnectionAdapter.getSession(),
						targetConnectionAdapter.getSession());
				filters.add(filterInstance);
				LOG.info("row set filter is created! from {}", filterDefinition);
				if (filterDefinition.maxRowsBatchSize != null
						&& (maxRowsBatchSize == null || filterDefinition.maxRowsBatchSize < maxRowsBatchSize)) {
					maxRowsBatchSize = filterDefinition.maxRowsBatchSize;
				}
			}
		}

		return new RowFilters(filters, maxRowsBatchSize);
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

	public String getName() {
		return name;
	}

	public int getIndex() {
		return index;
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
		effectivePageSize = tableMigrationDefinition.pageSize;
		pageSizeReduceIterationsApplied = 0;

		writeExecutorService = Executors.newFixedThreadPool(tableMigrationDefinition.parallelWriteRowCount);

		try {

			// Manual paging: each driver page is fetched explicitly via paging state.
			// This avoids relying on ResultSet.iterator() auto-fetch (where hasNext() can throw
			// on next-page read timeout) and makes page boundaries real for metrics/pause.
			SimpleStatement query = migratorPlugin.getReadQuery();
			ByteBuffer pagingState = null;

			while (!isMaxWriteRowCountReached()) {
				// Fetch fails before this page is processed/written — safe to retry same paging state
				ResultSet result = fetchNextPage(query, pagingState);

				// Consume only the already-fetched page. Do NOT use iterator()/all() —
				// those auto-fetch the next page. Sync ResultSet has no currentPage() in
				// driver 4.12; getAvailableWithoutFetching() + one() is the page-local API.
				int rowsInPage = result.getAvailableWithoutFetching();
				List<Row> pageRows = new ArrayList<>(rowsInPage);
				for (int i = 0; i < rowsInPage; i++) {
					Row row = result.one();
					if (row == null) {
						break;
					}
					pageRows.add(row);
					rowsRead++;
				}

				migratePageRows(pageRows);

				pagingState = result.getExecutionInfo().getPagingState();
				if (pagingState == null) {
					break;
				}

				if (tableMigrationDefinition.pauseMillisBetweenPages > 0) {
					LOG.debug("{}: page is exhausted - taking a break... ({} msec)", name,
							tableMigrationDefinition.pauseMillisBetweenPages);
					ThreadUtil.waitMillis(tableMigrationDefinition.pauseMillisBetweenPages);
				}

				if (System.currentTimeMillis() >= lastStatusPrintTime + printStatusMessageMillis) {
					printStatusLog();
				}
			}

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

			writeExecutorService.shutdownNow();
			writeExecutorService = null;
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
				"{}: started {} ago | time spent in reading/writing: {} (+{}) / {} (+{}) | pageFetchMean: {} msec (pageSize {}{}), writeBatchTookMean: {} msec (filterBatchSize {}) - with sliding window {} secs",
				name, TimeUtil.millisToHumanReadableString(System.currentTimeMillis() - startedTimestamp),
				TimeUtil.millisToHumanReadableString(millisSpentWithReading),
				TimeUtil.millisToHumanReadableString(deltaMillisSpentWithReading),
				TimeUtil.millisToHumanReadableString(millisSpentWithWriting),
				TimeUtil.millisToHumanReadableString(deltaMillisSpentWithWriting), pageFetchMillisMean,
				effectivePageSize > 0 ? effectivePageSize : tableMigrationDefinition.pageSize,
				effectivePageSize > 0 && effectivePageSize != tableMigrationDefinition.pageSize
						? " (configured " + tableMigrationDefinition.pageSize + ")"
						: "",
				writeBatchTookMillisMean,
				rowSetFilters.getMaxRowsBatchSize() != null ? rowSetFilters.getMaxRowsBatchSize() : "n/a (full page)",
				HISTOGRAMS_WINDOW_SECONDS);

		lastStatusPrintTime = System.currentTimeMillis();
	}

	/**
	 * Fetches the next page for the given query + paging state.
	 * <p>
	 * Applies {@link #readRetryStrategy} first; if that is exhausted for a retryable failure, may
	 * reduce {@link #effectivePageSize} via {@link #pageSizeReduceStrategy} and try again with the same
	 * paging state. Does not advance past a failed page — caller keeps the paging state until this
	 * returns successfully.
	 */
	private ResultSet fetchNextPage(SimpleStatement query, ByteBuffer pagingState) {
		while (true) {
			try {
				return executePageFetchWithReadRetries(query, pagingState, effectivePageSize);
			} catch (IllegalStateException e) {
				if (Thread.currentThread().isInterrupted() || e.getCause() instanceof InterruptedException) {
					throw e;
				}
				Throwable cause = e.getCause();
				if (!(cause instanceof Exception) || !isRetryableReadFailure((Exception) cause)) {
					throw e;
				}
				if (!tryReduceEffectivePageSize()) {
					throw e;
				}
				// same paging state, smaller page size, full read-retry budget again
			}
		}
	}

	/**
	 * One full {@link #readRetryStrategy} cycle at the given page size.
	 */
	private ResultSet executePageFetchWithReadRetries(SimpleStatement query, ByteBuffer pagingState, int pageSize) {
		RetryStrategy strategy = readRetryStrategy;
		int retryCount = 0;
		long pauseMillisBetweenRetries = 1000;
		long multiplier = 1;
		if (strategy != null) {
			retryCount = strategy.retryCount;
			pauseMillisBetweenRetries = strategy.pauseMillisBetweenRetries;
			multiplier = strategy.exponentialPauseMultiplier;
		}

		Exception lastFailure = null;
		while (retryCount >= 0) {
			retryCount--;
			long pageFetchStarted = System.currentTimeMillis();
			try {
				SimpleStatement pageQuery = query.setPageSize(pageSize);
				if (pagingState != null) {
					pageQuery = pageQuery.setPagingState(pagingState);
				}
				ResultSet result = sourceConnectionAdapter.getSession().execute(pageQuery);
				long pageFetchTookMillis = System.currentTimeMillis() - pageFetchStarted;
				pageFetchMillisHistogram.update(pageFetchTookMillis);
				millisSpentWithReading += pageFetchTookMillis;
				return result;
			} catch (Exception e) {
				lastFailure = e;
				millisSpentWithReading += System.currentTimeMillis() - pageFetchStarted;

				if (retryCount < 0) {
					break;
				}
				if (!isRetryableReadFailure(e)) {
					LOG.warn("{}: page fetch failed with non-retryable error - skipping readRetryStrategy", name, e);
					break;
				}

				LOG.warn(
						"{}: page fetch failed (pageSize {}) - will retry {} more time(s), now wait {} millis... cause: {}",
						name, pageSize, retryCount, pauseMillisBetweenRetries, e.toString());
				try {
					Thread.sleep(pauseMillisBetweenRetries);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("page fetch interrupted while waiting to retry", ie);
				}
				pauseMillisBetweenRetries *= multiplier;
			}
		}

		throw new IllegalStateException("page fetch failed after retries", lastFailure);
	}

	/**
	 * @return true if effective page size was reduced and caller should retry the same paging state
	 */
	private boolean tryReduceEffectivePageSize() {
		PageSizeReduceStrategy strategy = pageSizeReduceStrategy;
		if (strategy == null || strategy.maxIteration <= 0) {
			return false;
		}
		if (pageSizeReduceIterationsApplied >= strategy.maxIteration) {
			LOG.warn("{}: pageSize reduce maxIteration {} already reached (effective pageSize {})", name,
					strategy.maxIteration, effectivePageSize);
			return false;
		}

		int newPageSize = effectivePageSize / strategy.reducePageSizeFactor;
		if (newPageSize < 1 || newPageSize == effectivePageSize) {
			LOG.warn("{}: cannot reduce pageSize further from {} with factor {}", name, effectivePageSize,
					strategy.reducePageSizeFactor);
			return false;
		}

		LOG.warn("{}: read retries exhausted at pageSize {} - reducing pageSize to {} (reduce iteration {}/{})", name,
				effectivePageSize, newPageSize, pageSizeReduceIterationsApplied + 1, strategy.maxIteration);
		effectivePageSize = newPageSize;
		pageSizeReduceIterationsApplied++;
		return true;
	}

	/**
	 * Whether a failed page fetch is safe/sensible to retry. Failure happens before this page's rows
	 * are processed or written, so retrying the same paging state is safe even for counter tables.
	 */
	private boolean isRetryableReadFailure(Exception failure) {
		if (failure instanceof ReadTimeoutException || failure instanceof UnavailableException
				|| failure instanceof NoNodeAvailableException) {
			return true;
		}
		return failure.getClass().getName().toLowerCase().contains("timeout");
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
	 * Processes one driver page: optionally splits into filter-sized chunks when a filter declares
	 * {@code maxRowsBatchSize}, otherwise migrates the whole page in one go.
	 */
	private void migratePageRows(List<Row> pageRows) {
		if (pageRows.isEmpty() || isMaxWriteRowCountReached()) {
			return;
		}

		Integer maxRowsBatchSize = rowSetFilters.getMaxRowsBatchSize();
		if (maxRowsBatchSize == null || maxRowsBatchSize <= 0 || maxRowsBatchSize >= pageRows.size()) {
			migrateFetchedRows(pageRows);
			return;
		}

		int fromIndex = 0;
		while (fromIndex < pageRows.size() && !isMaxWriteRowCountReached()) {
			int toIndex = Math.min(fromIndex + maxRowsBatchSize, pageRows.size());
			migrateFetchedRows(pageRows.subList(fromIndex, toIndex));
			fromIndex = toIndex;
		}
	}

	/**
	 * The soul of migration. This method gets a set of Rows. It executes the filter on them and migrate
	 * what is passed the filter
	 *
	 * @param fetchedRows
	 * @throws IllegalStateException
	 *             in case error occured and we need to abort the full process
	 */
	private void migrateFetchedRows(List<Row> fetchedRows) throws IllegalStateException {

		long startedAt = System.currentTimeMillis();

		// first let's filter what we read (if there are filters)
		List<Row> filteredRows = fetchedRows;
		for (IRowSetFilter filter : rowSetFilters.getRowSetFilters()) {
			filteredRows = filter.filterRowSet(filteredRows);
		}
		rowsPassedFilter += filteredRows.size();

		// === Step 1 - let's assemble a list of WriteRowTasks for each row to migrate!

		Iterator<Row> rowsIterator = filteredRows.iterator();
		List<WriteRowTask> writeRowTasks = new ArrayList<>(filteredRows.size());
		while (rowsIterator.hasNext() && !isMaxWriteRowCountReached()) {
			Row row = rowsIterator.next();
			// creating a row write task and adding it to the list
			writeRowTasks.add(new WriteRowTask(tableMigrationDefinition, migratorPlugin, row, writeRetryStrategy));
		}

		// === Step 2 - now give them to the executor threads and wait for them to complete (or fail)

		try {

			if (!writeRowTasks.isEmpty()) {
				// now let's execute them all!
				List<Future<Boolean>> futures = new ArrayList<>(writeRowTasks.size());
				for (WriteRowTask task : writeRowTasks) {
					futures.add(writeExecutorService.submit(task));
				}

				// and now let's wait until all completes (or something happens...)
				boolean wait = true;
				while (wait) {

					// start with a small sleep...
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}

					// let's collect which is done!
					List<Future<Boolean>> futuresJustCompleted = futures.stream().filter(item -> {
						return item.isDone();
					}).collect(Collectors.toList());

					// remove all of them from the full list
					futures.removeAll(futuresJustCompleted);

					LOG.trace("{} - {} rows write just completed... {} more to go in the batch",
							tableMigrationDefinition.tableName, futuresJustCompleted.size(), futures.size());

					// now let's check which were completed now!
					for (Future<Boolean> futureCompleted : futuresJustCompleted) {
						try {
							if (futureCompleted.get()) {
								rowsMigrated++;
							}
						} catch (ExecutionException e) {
							// this stuff failed
							rowsFailed++;

							// should we abort everything?
							if (!tableMigrationDefinition.continueOnRowError) {
								LOG.error("row insert failed, aborting all non completed row migrations...");
								futures.forEach(futureItem -> {
									futureItem.cancel(true);
								});
								throw new IllegalStateException(
										"row insert failed and continueOnRowError=false so aborting...");
							}
						} catch (Exception e) {
							// and here what?
						}
					}

					wait = !futures.isEmpty();
				}
			}

		} catch (Exception e) {
			// if we get here we exit with exception
			throw new IllegalStateException("error occured - aborting full table migration", e);
		} finally {
			long writeTookMillis = System.currentTimeMillis() - startedAt;
			writeBatchTookMillisHistogram.update(writeTookMillis);
			millisSpentWithWriting += writeTookMillis;
		}

	}

	public final static class WriteRowTask implements Callable<Boolean> {

		public final IMigratorPlugin migratorPlugin;
		public final TableMigrationDefinition tableMigrationDefinition;
		public final Row row;
		private final RetryStrategy validatedWriteRetryStrategy;

		public WriteRowTask(TableMigrationDefinition tableMigrationDefinition, IMigratorPlugin migratorPlugin, Row row,
				RetryStrategy validatedWriteRetryStrategy) {
			this.tableMigrationDefinition = tableMigrationDefinition;
			this.migratorPlugin = migratorPlugin;
			this.row = row;
			this.validatedWriteRetryStrategy = validatedWriteRetryStrategy;
		}

		@Override
		public Boolean call() throws Exception {

			// 1 + 2 + 4 + 8 + 16 = 31s waiting time
			int retryCount = 0;
			long pauseMillisBetweenRetries = 1000;
			long multiplier = 1;
			if (validatedWriteRetryStrategy != null) {
				retryCount = validatedWriteRetryStrategy.retryCount;
				pauseMillisBetweenRetries = validatedWriteRetryStrategy.pauseMillisBetweenRetries;
				multiplier = validatedWriteRetryStrategy.exponentialPauseMultiplier;
			}

			Exception failure = null;
			while (retryCount >= 0) {
				retryCount--;
				failure = null;

				try {
					return migratorPlugin.migrateRow(row);
				} catch (Exception e) {
					failure = e;

					if (retryCount >= 0) {
						// hm... we failed...
						// we need to figure out whether we apply retry strategy or not
						boolean safeToRetry = isSafeToDoWriteRetry(failure);
						if (safeToRetry) {
							// OK its time to wait...
							LOG.warn("row migration failed - will retry {} times, now wait {} millis...", retryCount,
									pauseMillisBetweenRetries);
							try {
								Thread.sleep(pauseMillisBetweenRetries);
							} catch (InterruptedException ie) {
								// abort retries so Future.cancel(true) / shutdown can take effect
								Thread.currentThread().interrupt();
								break;
							}
							pauseMillisBetweenRetries *= multiplier;
						} else {
							LOG.warn(
									"skipping retry policy - target table is counter table and isSafeToDoWriteRetry() classified it unsafe to retry this error!");
						}

					}
				}

			}
			if (failure != null) {
				String msg = "row migration failed with exception";
				if (tableMigrationDefinition.continueOnRowError) {
					// this just a warning then
					LOG.warn(msg, failure);
				} else {
					// now its an error
					LOG.error(msg, failure);
				}
				throw new IllegalStateException(msg, failure);
			}

			return true;
		}

		private boolean isSafeToDoWriteRetry(Exception failure) {
			if (!tableMigrationDefinition._isTargetCounterTable) {
				// on non counter tables it is - go
				return true;
			}

			// so we have counter table...

			// for now, classify any kind of timeout unsafe! Might be too much but let's be on safe side first
			if ((failure instanceof WriteTimeoutException)
					|| failure.getClass().getName().toLowerCase().contains("timeout")) {
				// not a good idea if reason is timeout...
				return false;
			}

			return true;
		}
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
