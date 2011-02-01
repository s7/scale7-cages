package org.scale7.zookeeper.cages;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.scale7.concurrency.ManualResetEvent;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

public final class ZkSessionManager implements Watcher {
	private static final Logger logger = SystemProxy.getLoggerFromFactory(ZkSessionManager.class);

	volatile boolean shutdown;
	volatile ZooKeeper zooKeeper;
	final String connectString;
	final ManualResetEvent isConnected;
	final ExecutorService connectExecutor;
	final ScheduledExecutorService callbackExecutor;
	Set<ZkSyncPrimitive> currResurrectList;
	Set<ZkSyncPrimitive> currRestartOnConnectList;
	Integer retryMutex = new Integer(-1); // controls access to *current* retry lists
	final int sessionTimeout;
	int maxConnectAttempts;
	IOException exception;

	public ZkSessionManager(String connectString) {
		this(connectString, 6000, 5);
	}

	public ZkSessionManager(String connectString, int sessionTimeout, int maxConnectAttempts) {

		if (maxConnectAttempts < 1)
			throw new IllegalArgumentException("maxConnectAttempts must be greater than or equal to 0");

		shutdown = false;
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		this.maxConnectAttempts = maxConnectAttempts; // TODO should we use this? Throwing here will kill client. Better just keep on waiting..
		isConnected = new ManualResetEvent(false);
		callbackExecutor = Executors.newScheduledThreadPool(8);
		exception = null;

		connectExecutor = Executors.newSingleThreadExecutor();
		try {
			connectExecutor.submit(clientCreator).get(); // we know zooKeeper client assigned when past this statement
			isConnected.waitOne();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void shutdown() throws InterruptedException {
		shutdown = true;
		zooKeeper.close();
		callbackExecutor.shutdownNow();
	}

	public boolean isShutdown() {
		return shutdown;
	}

	void retryPrimitiveOperation(Runnable operation, int retries) {
		if (!shutdown) {
	    	int delay = 250 + retries * 500;
	    	if (delay > 7500)
	    		delay = 7500;
	    	callbackExecutor.schedule(operation, delay, TimeUnit.MILLISECONDS);
		}
	}

	void restartPrimitiveWhenConnected(ZkSyncPrimitive primitive) {
		synchronized (retryMutex) {
			if (currRestartOnConnectList == null)
				currRestartOnConnectList = Collections.newSetFromMap(new WeakHashMap<ZkSyncPrimitive, Boolean>());
			currRestartOnConnectList.add(primitive);
		}
	}

	void resurrectPrimitiveWhenNewSession(ZkSyncPrimitive primitive) {
		synchronized (retryMutex) {
			if (currResurrectList == null)
				currResurrectList = Collections.newSetFromMap(new WeakHashMap<ZkSyncPrimitive, Boolean>());
			currResurrectList.add(primitive);
		}
	}

	@Override
	public void process(WatchedEvent event)  {
		if (event.getType() == Event.EventType.None) {
    		KeeperState state = event.getState();
    		logger.debug("process {}", state);
    		switch (state) {
    		case SyncConnected:
    			onConnected();
    			break;
    		case Disconnected:
    			onDisconnection();
    			break;
    		case Expired:
    			onSessionExpired();
    			break;
    		}
		}
	}

	/**
	 * The ZooKeeper client is connected to the ZooKeeper cluster. Actions that modify cluster data may now be
	 * performed. Any primitives that were previously suspended after disconnection must be restarted, and any
	 * primitives that wished to be resurrected after session expiry, must be asked to resynchronize
	 */
	private void onConnected() {
		// We need to process the resurrection list
		synchronized (retryMutex) {
			// We are going to process the existing lists. We take a copy of the lists and reset them to null to
			// avoid potential re-entrancy problems if the client becomes disconnected again while restarting the
			// waiting primitives thus causing them to try to re-add themselves to these lists.
			Set<ZkSyncPrimitive> resurrectList = currResurrectList;
			Set<ZkSyncPrimitive> restartOnConnectList = currRestartOnConnectList;
			currResurrectList = null;
			currRestartOnConnectList = null;
			// Process resurrection list...
			if (resurrectList != null) {
				logger.info("onConnected processing currResurrectList.size {}", resurrectList.size());
				for (ZkSyncPrimitive primitive : resurrectList) {
					primitive.zooKeeper = this.zooKeeper; // assign new client
					primitive.resynchronize();
				}
			}
			// Process restart on re-connection list
			if (restartOnConnectList != null)
				for (ZkSyncPrimitive primitive : restartOnConnectList) {
					Runnable restart = primitive.retryOnConnect;
					primitive.retryOnConnect = null; // this may be re-assigned by running if disconnect again
					restart.run();
				}
		}
		isConnected.set();
	}

	/**
	 * We have been disconnected from ZooKeeper. The client will try to reconnect automatically. However, even
	 * after a successful reconnect, we may miss node creation followed by node deletion event. Furthermore, we
	 * cannot be *sure* of situation on server while in this state, nor perform actions that require modifying
	 * the server state. This may require special handling so we notify our sync objects.
	 */
	private void onDisconnection() {
		isConnected.reset();
	}

	/**
	 * The ZooKeeper session has expired. We need to initiate the creation of a new client session. Primitives
	 * that are currently suspended while waiting for re-connection must now be killed, except for the rare case
	 * where they can be resurrected when there is a new session.
	 */
	private void onSessionExpired() {
		synchronized (retryMutex) {
			// Primitives waiting for reconnection before continuing their operations must now die, except for the
			// rare case they wish to be resurrected when there is a new session
			if (currRestartOnConnectList != null) {
				for (ZkSyncPrimitive primitive : currRestartOnConnectList)
					if (primitive.shouldResurrectAfterSessionExpiry())
						currResurrectList.add(primitive);
					else
						primitive.die(Code.SESSIONEXPIRED);
				// Clear the reconnect list now
				currRestartOnConnectList.clear();
			}
		}
		isConnected.reset();
		connectExecutor.submit(clientCreator);
	}

	private Callable<ZooKeeper> clientCreator = new Callable<ZooKeeper> () {

		@Override
		public ZooKeeper call() throws IOException, InterruptedException {

			int attempts = 0;
			int retryDelay = 50;

			while (true) {
				try {
					zooKeeper = new ZooKeeper(connectString, sessionTimeout, ZkSessionManager.this);
					return zooKeeper;
				} catch (IOException e) {
					logger.error("Failed to connect client across network to required ensemble...");
					attempts++;
					if (maxConnectAttempts != 0 && attempts >= maxConnectAttempts)
						throw (IOException)e.getCause();
					retryDelay *= 2;
					if (retryDelay > 7500)
						retryDelay = 7500;
				}
				Thread.sleep(retryDelay);
			}
		}
	};

	private static ZkSessionManager instance;
	//private static Integer mutex = new Integer(-1);

	public static ZkSessionManager instance() {
		return instance;
	}

	public static void initializeInstance(String connectString) {
		instance = new ZkSessionManager(connectString);
	}

	public static void initializeInstance(String connectString, int sessionTimeout, int maxAttempts) {
		instance = new ZkSessionManager(connectString, sessionTimeout, maxAttempts);
	}
}
