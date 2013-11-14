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
/**
 *
 * Modified version of original class from HouseScream
 * 
 * https://github.com/housecream/server/blob/develop/server/ws/src/main/java/org/housecream/server/application/CassandraEmbedded.java
 * 
 */

package com.github.pmerienne.trident.state.cassandra.embedded;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CassandraEmbeddedServerStarter {
	CASSANDRA_EMBEDDED;

	private final Logger log = LoggerFactory.getLogger(CassandraEmbeddedServerStarter.class);

	private ExecutorService executor;

	public void start(final CassandraConfig config) {

		if (isAlreadyRunning()) {
			log.info("Cassandra is already running, not starting new one");
			return;
		}

		log.info("Starting Cassandra...");
		config.write();
		System.setProperty("cassandra.config", "file:" + config.getConfigFile().getAbsolutePath());
		System.setProperty("cassandra-foreground", "true");

		final CountDownLatch startupLatch = new CountDownLatch(1);
		executor = Executors.newSingleThreadExecutor();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					CassandraDaemon cassandraDaemon = new CassandraDaemon();
					cassandraDaemon.init(null);
					cassandraDaemon.start();
					startupLatch.countDown();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				// cassandraDaemon.activate();
			}
		});

		try {
			startupLatch.await(30, SECONDS);
		} catch (InterruptedException e) {
			log.error("Timeout starting Cassandra embedded", e);
			throw new IllegalStateException("Timeout starting Cassandra embedded", e);
		}
	}

	private boolean isAlreadyRunning() {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			MBeanInfo mBeanInfo = mbs.getMBeanInfo(new ObjectName("org.apache.cassandra.db:type=StorageService"));
			if (mBeanInfo != null) {
				return true;
			}
			return false;
		} catch (InstanceNotFoundException e) {
			return false;
		} catch (IntrospectionException e) {
			throw new IllegalStateException("Cannot check if cassandra is already running", e);
		} catch (MalformedObjectNameException e) {
			throw new IllegalStateException("Cannot check if cassandra is already running", e);
		} catch (ReflectionException e) {
			throw new IllegalStateException("Cannot check if cassandra is already running", e);
		}

	}

}
