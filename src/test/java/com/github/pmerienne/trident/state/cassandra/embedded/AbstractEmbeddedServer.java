/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.trident.state.cassandra.embedded;

import static com.github.pmerienne.trident.state.cassandra.embedded.CassandraConfig.*;
import static com.github.pmerienne.trident.state.cassandra.embedded.CassandraEmbeddedConfigParameters.*;
import static com.github.pmerienne.trident.state.cassandra.embedded.CassandraEmbeddedServerStarter.CASSANDRA_EMBEDDED;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class AbstractEmbeddedServer {
	;

	public static final String CASSANDRA_HOST = "cassandraHost";

	public static final Logger log = LoggerFactory.getLogger(AbstractEmbeddedServer.class);

	private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private static int cqlPort;

    private static int thriftPort;

    public static int getThriftPort() {
        return thriftPort;
    }

    public static int getCqlPort() {
        return cqlPort;
    }

	protected void startServer(Map<String, Object> parameters) {
		String cassandraHost = System.getProperty(CASSANDRA_HOST);
		if (StringUtils.isBlank(cassandraHost)) {

			validateDataFolders(parameters);
			cleanCassandraDataFiles(parameters);
			cleanCassandraConfigFile(parameters);
			randomizePortsIfNeeded(parameters);

			CassandraConfig cassandraConfig = new CassandraConfig(parameters);

			log.info(" Random embedded Cassandra RPC port/Thrift port = {}", cassandraConfig.getRPCPort());
			log.info(" Random embedded Cassandra Native port/CQL3 port = {}", cassandraConfig.getCqlPort());
			log.info(" Random embedded Cassandra Storage port = {}", cassandraConfig.getStoragePort());
			log.info(" Random embedded Cassandra Storage SSL port = {}", cassandraConfig.getStorageSSLPort());

			// Start embedded server
			CASSANDRA_EMBEDDED.start(cassandraConfig);
		}
	}

	protected String extractAndValidateKeyspaceName(Map<String, Object> parameters) {
		String keyspaceName = (String) parameters.get(KEYSPACE_NAME);
		assert StringUtils.isNotBlank(keyspaceName) : "The provided keyspace name should not be blank";
		assert KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches() : String.format(
				"The provided keyspace name '%s' should match the " + "following pattern : '%s'", keyspaceName,
				KEYSPACE_NAME_PATTERN.pattern());

		return keyspaceName;
	}

	private void validateDataFolders(Map<String, Object> parameters) {
		final String dataFolder = (String) parameters.get(DATA_FILE_FOLDER);
		final String commitLogFolder = (String) parameters.get(COMMIT_LOG_FOLDER);
		final String savedCachesFolder = (String) parameters.get(SAVED_CACHES_FOLDER);

		log.info(" Embedded Cassandra data directory = {}", dataFolder);
		log.info(" Embedded Cassandra commitlog directory = {}", commitLogFolder);
		log.info(" Embedded Cassandra saved caches directory = {}", savedCachesFolder);

		validateFolder(dataFolder);
		validateFolder(commitLogFolder);
		validateFolder(savedCachesFolder);

	}

	private void validateFolder(String folderPath) {
		String currentUser = System.getProperty("user.name");
		final File folder = new File(folderPath);
		if (!DEFAULT_TEST_FOLDERS.contains(folderPath)) {
			assert folder.exists() : String.format("Folder '%s' does not exist", folder.getAbsolutePath());
			assert folder.isDirectory() : String.format("Folder '%s' is not a directory", folder.getAbsolutePath());
			assert folder.canRead() : String.format("No read credential. Please grant read permission for the current"
					+ " " + "user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
			assert folder.canWrite() : String.format("No write credential. Please grant write permission for the "
					+ "current " + "user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
		}
	}

	private void cleanCassandraDataFiles(Map<String, Object> parameters) {
		if ((Boolean) parameters.get(CLEAN_CASSANDRA_DATA_FILES)) {
			final ImmutableSet<String> dataFolders = ImmutableSet.<String> builder()
					.add((String) parameters.get(DATA_FILE_FOLDER)).add((String) parameters.get(COMMIT_LOG_FOLDER))
					.add((String) parameters.get(SAVED_CACHES_FOLDER)).build();
			for (String dataFolder : dataFolders) {
				File dataFolderFile = new File(dataFolder);
				if (dataFolderFile.exists() && dataFolderFile.isDirectory()) {
					log.info("Cleaning up embedded Cassandra data directory '{}'", dataFolderFile.getAbsolutePath());
					FileUtils.deleteQuietly(dataFolderFile);
				}
			}
		}
	}

	private void cleanCassandraConfigFile(Map<String, Object> parameters) {
		if ((Boolean) parameters.get(CLEAN_CASSANDRA_CONFIG_FILE)) {
			String configYamlFilePath = (String) parameters.get(CONFIG_YAML_FILE);
			final File configYamlFile = new File(configYamlFilePath);
			if (configYamlFile.exists()) {
				String currentUser = System.getProperty("user.name");
				assert configYamlFile.canWrite() : String.format(
						"No write credential. Please grant write permission for "
								+ "the current user '%s' on file '%s'", currentUser, configYamlFile.getAbsolutePath());
				configYamlFile.delete();
			}
		}
	}

	private void randomizePortsIfNeeded(Map<String, Object> parameters) {
		final Integer thriftPort = extractAndValidatePort(Optional.fromNullable(parameters.get(CASSANDRA_THRIFT_PORT))
				.or(thriftRandomPort()), CASSANDRA_THRIFT_PORT);
		final Integer cqlPort = extractAndValidatePort(
				Optional.fromNullable(parameters.get(CASSANDRA_CQL_PORT)).or(cqlRandomPort()), CASSANDRA_CQL_PORT);
		final Integer storagePort = extractAndValidatePort(Optional
				.fromNullable(parameters.get(CASSANDRA_STORAGE_PORT)).or(storageRandomPort()), CASSANDRA_STORAGE_PORT);
		final Integer storageSSLPort = extractAndValidatePort(
				Optional.fromNullable(parameters.get(CASSANDRA_STORAGE_SSL_PORT)).or(storageSslRandomPort()),
				CASSANDRA_STORAGE_SSL_PORT);

		parameters.put(CASSANDRA_THRIFT_PORT, thriftPort);
		parameters.put(CASSANDRA_CQL_PORT, cqlPort);
		parameters.put(CASSANDRA_STORAGE_PORT, storagePort);
		parameters.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        AbstractEmbeddedServer.cqlPort = cqlPort;
        AbstractEmbeddedServer.thriftPort = thriftPort;
	}

	private Integer extractAndValidatePort(Object port, String portLabel) {
		assert port instanceof Integer : String.format("The provided '%s' port should be an integer", portLabel);
		assert (Integer) port > 0 : String.format("The provided '%s' port should positive", portLabel);
		return (Integer) port;

	}
}
