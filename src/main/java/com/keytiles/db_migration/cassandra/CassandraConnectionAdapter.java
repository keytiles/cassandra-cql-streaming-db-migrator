package com.keytiles.db_migration.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * This class hides the concrete Cassandra driver - acts as an adapter in between our world and the
 * concrete driver. Its goal is to provide a session - initialized against a given Keyspace
 * <p>
 * There are (and in the future will have more) optional properties you can set. Whatever is
 * mandatory you add it to the CTOR
 * <p>
 * After object is created you have to connect it by invoking {@link #connect()} -
 * {@link #getSession()} returns a connected session only afterwards. Once you do not need it
 * anymore you have to close this object by invoking {@link #close()}
 *
 * @author AttilaW
 *
 */
public class CassandraConnectionAdapter implements Closeable {

	protected final static Logger LOG = LoggerFactory.getLogger(CassandraConnectionAdapter.class);

	private final String name;
	private final List<String> contactNodes;
	private final String contactNodesDatacenterName;
	private String defaultKeyspaceName;
	private String applicationName;

	private Long reconnectionDelays;

	// driver default is 5000
	private Integer defaultPageSize = 3000;

	// driver default is: 2 seconds
	private Integer requestTimeout = 10000;
	// driver default is: 2 seconds so same as requestTimeout
	private Integer continousPagingTimeoutFirstPage;
	// driver default is: 1 seconds so 50% of requestTimeout
	private Integer continousPagingTimeoutOtherPages;

	private CqlSession session;

	private final MetricRegistry metricRegistry;

	/**
	 * @param name
	 *            The name of this connection. Due to Metrics feature of Cassandra drivers it is a good
	 *            idea to give a unique name JVM/System wide
	 * @param contactNodes
	 *            comma separated list of "&lt;host-ip&gt;:&lt;port&gt;" formatted contact point
	 *            references for all contact points
	 * @param contactNodesDatacenterName
	 *            All contact points must belong to it (as reported in their system tables:
	 *            system.local.data_center and system.peers.data_center)
	 */
	public CassandraConnectionAdapter(String name, String contactNodes, String contactNodesDatacenterName) {
		this(name, contactNodes, contactNodesDatacenterName, null);
	}

	public CassandraConnectionAdapter(String name, String contactNodes, String contactNodesDatacenterName,
			MetricRegistry metricRegistry) {
		this.name = name;
		this.contactNodes = Splitter.on(",").trimResults().splitToList(contactNodes);
		this.contactNodesDatacenterName = contactNodesDatacenterName;
		this.metricRegistry = metricRegistry;
	}

	public MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}

	public String getName() {
		return name;
	}

	public String getDefaultKeyspaceName() {
		return defaultKeyspaceName;
	}

	public void setDefaultKeyspaceName(String defaultKeyspaceName) {
		this.defaultKeyspaceName = defaultKeyspaceName;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public String getContactNodesDatacenterName() {
		return contactNodesDatacenterName;
	}

	public String getDatacenterName() {
		return contactNodesDatacenterName;
	}

	public Long getReconnectionDelays() {
		return reconnectionDelays;
	}

	public void setReconnectionDelays(Long reconnectionDelays) {
		Preconditions.checkArgument(reconnectionDelays == null || reconnectionDelays > 0, "Value can not be negative");
		this.reconnectionDelays = reconnectionDelays;
	}

	public Integer getDefaultPageSize() {
		return defaultPageSize;
	}

	public void setDefaultPageSize(Integer defaultPageSize) {
		Preconditions.checkArgument(defaultPageSize == null || defaultPageSize > 0, "Value can not be negative");
		this.defaultPageSize = defaultPageSize;
	}

	public Integer getRequestTimeout() {
		return requestTimeout;
	}

	public void setRequestTimeout(Integer requestTimeout) {
		Preconditions.checkArgument(requestTimeout == null || requestTimeout > 0, "Value can not be negative");
		this.requestTimeout = requestTimeout;
	}

	public Integer getContinousPagingTimeoutFirstPage() {
		return continousPagingTimeoutFirstPage;
	}

	public void setContinousPagingTimeoutFirstPage(Integer continousPagingTimeoutFirstPage) {
		Preconditions.checkArgument(continousPagingTimeoutFirstPage == null || continousPagingTimeoutFirstPage > 0,
				"Value can not be negative");
		this.continousPagingTimeoutFirstPage = continousPagingTimeoutFirstPage;
	}

	public Integer getContinousPagingTimeoutOtherPages() {
		return continousPagingTimeoutOtherPages;
	}

	public void setContinousPagingTimeoutOtherPages(Integer continousPagingTimeoutOtherPages) {
		Preconditions.checkArgument(continousPagingTimeoutOtherPages == null || continousPagingTimeoutOtherPages > 0,
				"Value can not be negative");
		this.continousPagingTimeoutOtherPages = continousPagingTimeoutOtherPages;
	}

	public List<String> getContactNodes() {
		return contactNodes;
	}

	public String getKeyspaceName() {
		return defaultKeyspaceName;
	}

	public synchronized boolean isConnected() {
		return session != null;
	}

	/**
	 * This will create a {@link CqlSession} object kept internally which you can get later using
	 * {@link #getSession()} method
	 *
	 */
	public synchronized void connect() {

		OptionsMap driverOptions = OptionsMap.driverDefaults();

		if (requestTimeout != null) {
			driverOptions.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(requestTimeout));
			driverOptions.put(TypedDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE, Duration.ofMillis(
					continousPagingTimeoutFirstPage != null ? continousPagingTimeoutFirstPage : requestTimeout));
			driverOptions.put(TypedDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES, Duration.ofMillis(
					continousPagingTimeoutOtherPages != null ? continousPagingTimeoutOtherPages : requestTimeout / 2));
		}

		/*
		 * for now I switch this off
		 * see: https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/native_protocol/
		 *
		driverOptions.put(TypedDriverOption.PROTOCOL_VERSION, ProtocolVersion.V5.name());
		 */
		// attilaw: problem again... setting explicitly to V4...
		// as with Cassandra 4.0-rc1 this is needed maybe negotiation does not work
		// and this way we are also OK with Cassandra 4.0 beta2
		driverOptions.put(TypedDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.name());

		driverOptions.put(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY, ConsistencyLevel.LOCAL_ONE.name());
		driverOptions.put(TypedDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.LOCAL_ONE.name());
		driverOptions.put(TypedDriverOption.CONTINUOUS_PAGING_PAGE_SIZE, defaultPageSize);
		if (reconnectionDelays != null && reconnectionDelays >= 0) {
			driverOptions.put(TypedDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(reconnectionDelays));
		}

		// let's name this session!
		driverOptions.put(TypedDriverOption.SESSION_NAME, name);

		// see: https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/
		CqlSessionBuilder sessionBuilder = CqlSession.builder() //
				.addContactPoints(toSocketAddressList(contactNodes)) //
				.withLocalDatacenter(contactNodesDatacenterName) //
				.withConfigLoader(DriverConfigLoader.fromMap(driverOptions));
		if (this.metricRegistry != null) {
			sessionBuilder.withMetricRegistry(metricRegistry);
		}
		if (StringUtils.isNotBlank(defaultKeyspaceName)) {
			sessionBuilder.withKeyspace(defaultKeyspaceName);
		}
		if (StringUtils.isNotBlank(applicationName)) {
			sessionBuilder.withApplicationName(applicationName);
		}

		session = sessionBuilder.build();

	}

	/**
	 * To get back a connected session
	 * <p>
	 * Ideally the {@link #connect()} method was invoked earlier but if not then it will be done now
	 *
	 * @return the connected usable session
	 */
	public synchronized CqlSession getSession() {
		if (session == null) {
			connect();
		}

		return this.session;
	}

	@Override
	public synchronized void close() throws IOException {
		LOG.info("closing connection... {}", this);
		if (session != null) {
			session.close();
			session = null;
		} else {
			LOG.info("session not connected {}", this);
		}
		LOG.info("closed! {}", this);
	}

	private List<InetSocketAddress> toSocketAddressList(List<String> hostPortList) {
		List<InetSocketAddress> socketAddresses = new ArrayList<>();
		for (String hostPort : hostPortList) {
			String[] parts = hostPort.split(":");
			if (parts.length != 2) {
				throw new RuntimeException(
						"Config error! host:port is not valid! value: '" + hostPort + "' in setup: " + hostPortList);
			}
			InetSocketAddress socketAddress = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
			socketAddresses.add(socketAddress);
		}
		return socketAddresses;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CassandraConnectionAdapter [contactNodes=").append(contactNodes)
				.append(", defaultKeyspaceName=").append(defaultKeyspaceName).append("]");
		return builder.toString();
	}

}