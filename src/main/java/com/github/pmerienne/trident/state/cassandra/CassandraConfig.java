package com.github.pmerienne.trident.state.cassandra;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.github.pmerienne.trident.state.cassandra.dao.CassandraDao;
import com.github.pmerienne.trident.state.serializer.SerializerFactory.SerializerType;

public class CassandraConfig extends HashMap<String, String> {

	public static final String STORM_CONFIG_KEY = "cassandra.state.config";

	public static final String CONTACT_POINTS = "cassandra.contact.points";

	public static final String PORT = "cassandra.host";

	public static final String KEYSPACE = "cassandra.keyspace";

	public static final String USERNAME = "cassandra.username";

	public static final String PASSWORD = "cassandra.password";

	public static final String SERIALIZER_TYPE = "cassandra.serializer";

	public static final String DEFAULT_CONTACT_POINTS = "localhost";

	public static final Integer DEFAULT_PORT = 9042;

	public static final String DEFAULT_KEYSPACE = "trident";

	public final static SerializerType DEFAULT_SERIALIZER = SerializerType.BINARY;

	private static final long serialVersionUID = -1913847520446822065L;

	private static final Object SEMAPHORE = new Object();

	private static CassandraDao dao;

	public CassandraConfig() {
	}

	public void initCassandraClient() {
		synchronized (SEMAPHORE) {
			if (dao == null) {
				createDao();
			}
		}
	}

	private void createDao() {
		String[] contactPoints = StringUtils.split(this.getContactPoints(), ",");
		Cluster cluster;
		String username = this.getUsername();
		String password = this.getPassword();

		if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
			cluster = Cluster.builder().addContactPoints(contactPoints).withPort(this.getPort())
					.withCredentials(username, password).build();
		} else {
			cluster = Cluster.builder().addContactPoints(contactPoints).withPort(this.getPort()).build();
		}

		Session session = cluster.connect(this.getKeyspace());
		dao = new CassandraDao(session);
	}

	public String getContactPoints() {
		return getString(CONTACT_POINTS, DEFAULT_CONTACT_POINTS);
	}

	public void setContactPoints(String contactPoints) {
		put(CONTACT_POINTS, contactPoints);
	}

	public Integer getPort() {
		return getInteger(PORT, DEFAULT_PORT);
	}

	public void setPort(Integer port) {
		put(PORT, Integer.toString(port));
	}

	public String getKeyspace() {
		return getString(KEYSPACE, DEFAULT_KEYSPACE);
	}

	public void setKeyspace(String keyspace) {
		put(KEYSPACE, keyspace);
	}

	public String getUsername() {
		return getString(USERNAME, null);
	}

	public void setUsername(String username) {
		put(USERNAME, username);
	}

	public String getPassword() {
		return getString(PASSWORD, null);
	}

	public void setPassword(String password) {
		put(PASSWORD, password);
	}

	public SerializerType getSerializerType() {
		return get(SERIALIZER_TYPE, DEFAULT_SERIALIZER);
	}

	private <T> T get(String key, T defaultValue) {
		return containsKey(key) ? ((T) get(key)) : defaultValue;
	}

	private String getString(String key, String defaultValue) {
		return containsKey(key) ? get(key) : defaultValue;
	}

	private Integer getInteger(String key, Integer defaultValue) {
		return containsKey(key) ? Integer.parseInt(get(key)) : defaultValue;
	}

	public CassandraDao getDao() {
		if (dao == null)
			createDao();
		return dao;
	}

	@SuppressWarnings({ "rawtypes" })
	public static CassandraConfig getFromStormConfig(Map conf) {
		CassandraConfig cassandraConfig;
		if (conf.containsKey(STORM_CONFIG_KEY)) {
			cassandraConfig = (CassandraConfig) conf.get(STORM_CONFIG_KEY);
		} else {
			cassandraConfig = new CassandraConfig();
		}
		return cassandraConfig;
	}
}
