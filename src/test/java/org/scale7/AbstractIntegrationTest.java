package org.scale7;

import java.io.File;

import junit.framework.TestCase;

public abstract class AbstractIntegrationTest extends TestCase {

	protected final String zkAddress = "localhost";

	protected final int zkPort = 11221;

	protected final String zkBaseDir = "target";

	private static EmbeddedZooKeeperServer zookeeperServer;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		if (zookeeperServer == null) {
			String tmpDir = zkBaseDir + "/ZK-" + System.currentTimeMillis();

			new File(tmpDir).mkdir();

			zookeeperServer = new EmbeddedZooKeeperServer(zkAddress, zkPort,
					tmpDir);
			zookeeperServer.start();
		}
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();

		if (zookeeperServer != null) {
			zookeeperServer.stop();
			zookeeperServer = null;
		}
	}
}
