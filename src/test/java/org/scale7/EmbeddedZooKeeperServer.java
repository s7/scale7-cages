package org.scale7;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.scale7.zookeeper.cages.ZkSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedZooKeeperServer {

	private static final Logger LOG = LoggerFactory
			.getLogger(EmbeddedZooKeeperServer.class);

	protected NIOServerCnxn.Factory serverFactory = null;
	protected Object mutex = new Object();
	protected File tmpDir;
	protected File baseDir;
	protected String host = "localhost";
	protected int port = 11221;

	public int CONNECTION_TIMEOUT = 30000;

	public EmbeddedZooKeeperServer(String host, int port, String baseDirectory) {
		this.baseDir = new File(baseDirectory);
		this.host = host;
		this.port = port;
	}

	/**
	 * Send the 4letterword
	 * 
	 * @param host
	 *            the destination host
	 * @param port
	 *            the destination port
	 * @param cmd
	 *            the 4letterword
	 * @return
	 * @throws IOException
	 */
	public String send4LetterWord(String cmd) throws IOException {
		Socket sock = new Socket(host, port);
		BufferedReader reader = null;
		try {
			OutputStream outstream = sock.getOutputStream();
			outstream.write(cmd.getBytes());
			outstream.flush();
			// this replicates NC - close the output stream before reading
			sock.shutdownOutput();

			reader = new BufferedReader(new InputStreamReader(
					sock.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
			return sb.toString();
		} finally {
			sock.close();
			if (reader != null) {
				reader.close();
			}
		}
	}

	public boolean waitForServerUp(long timeout) {
		long start = System.currentTimeMillis();
		while (true) {
			try {
				// if there are multiple hostports, just take the first one
				String result = send4LetterWord("stat");
				if (result.startsWith("Zookeeper version:")) {
					return true;
				}
			} catch (IOException e) {
				// ignore as this is expected
				LOG.info("server " + host + ":" + port + " not up " + e);
			}

			if (System.currentTimeMillis() > start + timeout) {
				break;
			}
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		return false;
	}

	public boolean waitForServerDown(long timeout) {
		long start = System.currentTimeMillis();
		while (true) {
			try {
				send4LetterWord("stat");
			} catch (IOException e) {
				return true;
			}

			if (System.currentTimeMillis() > start + timeout) {
				break;
			}
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				// ignore
			}
		}
		return false;
	}

	File createTmpDir(File parentDir) throws IOException {
		File tmpFile = File.createTempFile("ZooKeeper", ".embedded", parentDir);
		File tmpDir = new File(tmpFile + ".dir");
		tmpDir.mkdirs();
		return tmpDir;
	}

	NIOServerCnxn.Factory createNewServerInstance(File dataDir,
			NIOServerCnxn.Factory factory) throws IOException,
			InterruptedException {
		ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataDir, 3000);
		if (factory == null) {
			factory = new NIOServerCnxn.Factory(new InetSocketAddress(host,
					port), 0);
		}
		factory.startup(zks);
		waitForServerUp(CONNECTION_TIMEOUT);
		return factory;
	}

	void shutdownServerInstance(NIOServerCnxn.Factory factory) {
		if (factory != null) {
			ZKDatabase zkDb = factory.getZooKeeperServer().getZKDatabase();
			factory.shutdown();
			try {
				zkDb.close();
			} catch (IOException ie) {
				LOG.warn("Error closing logs ", ie);
			}
			waitForServerDown(CONNECTION_TIMEOUT);
		}
	}

	public boolean recursiveDelete(File d) {
		if (d.isDirectory()) {
			File children[] = d.listFiles();
			for (File f : children) {
				recursiveDelete(f);
			}
		}
		return d.delete();
	}

	public void setupTestEnv() {
		// during the tests we run with 100K prealloc in the logs.
		// on windows systems prealloc of 64M was seen to take ~15seconds
		// resulting in test failure (client timeout on first session).
		// set env and directly in order to handle static init/gc issues
		System.setProperty("zookeeper.preAllocSize", "100");
		FileTxnLog.setPreallocSize(100 * 1024);
	}

	protected void startServer() throws Exception {
		serverFactory = createNewServerInstance(tmpDir, serverFactory);
	}

	protected void stopServer() throws Exception {
		shutdownServerInstance(serverFactory);
		serverFactory = null;
	}

	public void start() throws Exception {
		setupTestEnv();

		tmpDir = createTmpDir(baseDir);

		startServer();
	}

	public void stop() throws Exception {
		stopServer();

		if (tmpDir != null) {
			recursiveDelete(tmpDir);
		}

		if (ZkSessionManager.instance() != null) {
			if (!ZkSessionManager.instance().isShutdown()) {
				ZkSessionManager.instance().shutdown();
			}
		}
	}
}
