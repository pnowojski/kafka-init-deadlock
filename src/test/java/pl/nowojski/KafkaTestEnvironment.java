/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.nowojski;

import kafka.admin.AdminUtils;
import kafka.common.KafkaException;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import scala.collection.mutable.ArraySeq;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * An implementation of the KafkaServerProvider for Kafka 0.11 .
 */
public class KafkaTestEnvironment {
	public static final int KAFKA_SERVERS_NUMBER = 3;
	public static final String KAFKA_HOST = "localhost";

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaTestEnvironment.class);

	private File tmpZkDir;
	private File tmpKafkaParent;
	private List<File> tmpKafkaDirs;
	private List<KafkaServer> brokers;
	private TestingServer zookeeper;
	private String zookeeperConnectionString;
	private String brokerConnectionString = "";
	private Properties standardProps;
	// 6 seconds is default. Seems to be too small for travis. 30 seconds
	private int zkTimeout = 30000;

	public String getBrokerConnectionString() {
		return brokerConnectionString;
	}

	public Properties getStandardProperties() {
		return standardProps;
	}

	public List<KafkaServer> getBrokers() {
		return brokers;
	}


	public void prepare() {
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		tmpZkDir = new File(tempDir, "kafkaITcase-zk-dir-" + (UUID.randomUUID().toString()));
		assertTrue("cannot create zookeeper temp dir", tmpZkDir.mkdirs());

		tmpKafkaParent = new File(tempDir, "kafkaITcase-kafka-dir-" + (UUID.randomUUID().toString()));
		assertTrue("cannot create kafka temp dir", tmpKafkaParent.mkdirs());

		tmpKafkaDirs = new ArrayList<>(KAFKA_SERVERS_NUMBER);
		for (int i = 0; i < KAFKA_SERVERS_NUMBER; i++) {
			File tmpDir = new File(tmpKafkaParent, "server-" + i);
			assertTrue("cannot create kafka temp dir", tmpDir.mkdir());
			tmpKafkaDirs.add(tmpDir);
		}

		zookeeper = null;
		brokers = null;

		try {
			zookeeper = new TestingServer(-1, tmpZkDir);
			zookeeperConnectionString = zookeeper.getConnectString();
			LOG.info("Starting Zookeeper with zookeeperConnectionString: {}", zookeeperConnectionString);

			LOG.info("Starting KafkaServer");
			brokers = new ArrayList<>(KAFKA_SERVERS_NUMBER);

			ListenerName listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
			for (int i = 0; i < KAFKA_SERVERS_NUMBER; i++) {
				KafkaServer kafkaServer = getKafkaServer(i, tmpKafkaDirs.get(i));
				brokers.add(kafkaServer);
				brokerConnectionString += KAFKA_HOST + ":" + kafkaServer.socketServer().boundPort(listenerName);
				brokerConnectionString +=  ",";
			}

			LOG.info("ZK and KafkaServer started.");
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Test setup failed: " + t.getMessage());
		}

		standardProps = new Properties();
		standardProps.setProperty("bootstrap.servers", brokerConnectionString);
	}

	public void shutdown() {
		for (KafkaServer broker : brokers) {
			if (broker != null) {
				broker.shutdown();
			}
		}
		brokers.clear();

		if (zookeeper != null) {
			try {
				zookeeper.stop();
			}
			catch (Exception e) {
				LOG.warn("ZK.stop() failed", e);
			}
			zookeeper = null;
		}

		// clean up the temp spaces

		if (tmpKafkaParent != null && tmpKafkaParent.exists()) {
			try {
				FileUtils.deleteDirectory(tmpKafkaParent);
			}
			catch (Exception e) {
				// ignore
			}
		}
		if (tmpZkDir != null && tmpZkDir.exists()) {
			try {
				FileUtils.deleteDirectory(tmpZkDir);
			}
			catch (Exception e) {
				// ignore
			}
		}
	}

	public ZkUtils getZkUtils() {
		ZkClient creator = new ZkClient(zookeeperConnectionString, zkTimeout, zkTimeout, new ZooKeeperStringSerializer());
		return ZkUtils.apply(creator, false);
	}

	public void createTestTopic(String topic, int numberOfPartitions, int replicationFactor, Properties topicConfig) {
		// create topic with one client
		LOG.info("Creating topic {}", topic);

		ZkUtils zkUtils = getZkUtils();
		try {
			AdminUtils.createTopic(zkUtils, topic, numberOfPartitions, replicationFactor, topicConfig, kafka.admin.RackAwareMode.Enforced$.MODULE$);
		} finally {
			zkUtils.close();
		}

		// validate that the topic has been created
		final long deadline = System.nanoTime() + 30_000_000_000L;
		do {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// restore interrupted state
			}
			// we could use AdminUtils.topicExists(zkUtils, topic) here, but it's results are
			// not always correct.

			// create a new ZK utils connection
			ZkUtils checkZKConn = getZkUtils();
			if (AdminUtils.topicExists(checkZKConn, topic)) {
				checkZKConn.close();
				return;
			}
			checkZKConn.close();
		}
		while (System.nanoTime() < deadline);
		fail("Test topic could not be created");
	}

	public void deleteTestTopic(String topic) {
		ZkUtils zkUtils = getZkUtils();
		try {
			LOG.info("Deleting topic {}", topic);

			ZkClient zk = new ZkClient(zookeeperConnectionString, zkTimeout, zkTimeout, new ZooKeeperStringSerializer());

			AdminUtils.deleteTopic(zkUtils, topic);

			zk.close();
		} finally {
			zkUtils.close();
		}
	}

	public static int getAvailablePort() {
		for (int i = 0; i < 50; i++) {
			try (ServerSocket serverSocket = new ServerSocket(0)) {
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			}
			catch (IOException ignored) {}
		}

		throw new RuntimeException("Could not find a free permitted port on the machine.");
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed).
	 */
	protected KafkaServer getKafkaServer(int brokerId, File tmpFolder) throws Exception {
		Properties kafkaProperties = new Properties();

		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", KAFKA_HOST);
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpFolder.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		kafkaProperties.put("message.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("replica.fetch.max.bytes", String.valueOf(50 * 1024 * 1024));
		kafkaProperties.put("transaction.max.timeout.ms", Integer.toString(1000 * 60 * 60 * 2)); // 2hours

		// for CI stability, increase zookeeper session timeout
		kafkaProperties.put("zookeeper.session.timeout.ms", zkTimeout);
		kafkaProperties.put("zookeeper.connection.timeout.ms", zkTimeout);

		final int numTries = 5;

		for (int i = 1; i <= numTries; i++) {
			int kafkaPort = getAvailablePort();
			kafkaProperties.put("port", Integer.toString(kafkaPort));

			KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

			try {
				scala.Option<String> stringNone = scala.Option.apply(null);
				KafkaServer server = new KafkaServer(kafkaConfig, Time.SYSTEM, stringNone, new ArraySeq<KafkaMetricsReporter>(0));
				server.startup();
				return server;
			}
			catch (KafkaException e) {
				if (e.getCause() instanceof BindException) {
					// port conflict, retry...
					LOG.info("Port conflict when starting Kafka Broker. Retrying...");
				}
				else {
					throw e;
				}
			}
		}

		throw new Exception("Could not start Kafka after " + numTries + " retries due to port conflicts.");
	}

	public void restartBroker(int brokerId) throws Exception {
		brokers.set(brokerId, getKafkaServer(brokerId, tmpKafkaDirs.get(brokerId)));
	}
}
