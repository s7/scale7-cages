package org.wyki.zookeeper.cages;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.wyki.concurrency.ManualResetEvent;

public final class ZkSessionManager implements Watcher {
	volatile boolean shutdown;	
	volatile ZooKeeper zooKeeper;
	final String connectString;
	final ManualResetEvent isConnected;
	final ExecutorService connectExecutor;
	final ScheduledExecutorService callbackExecutor;
	final Set<ZkSyncPrimitive> resurrectList;
	final int sessionTimeout;
	int maxConnectAttempts;
	IOException exception;
		
	public ZkSessionManager(String connectString) throws InterruptedException, IOException, ExecutionException {
		this(connectString, 6000, 5);
	}
	
	public ZkSessionManager(String connectString, int sessionTimeout, int maxConnectAttempts) throws InterruptedException, IOException, ExecutionException {
		
		if (maxConnectAttempts < 1)
			throw new IllegalArgumentException("maxConnectAttempts must be greater than or equal to 0");
		
		shutdown = false;		
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		this.maxConnectAttempts = maxConnectAttempts; // TODO should we use this? Throwing here will kill client. Better just keep on waiting..
		isConnected = new ManualResetEvent(false);
		callbackExecutor = Executors.newScheduledThreadPool(8);
		resurrectList = Collections.newSetFromMap(new WeakHashMap<ZkSyncPrimitive, Boolean>());
		exception = null;

		connectExecutor = Executors.newSingleThreadExecutor();
		connectExecutor.submit(clientCreator).get(); // we know zooKeeper client assigned when past this statement
		isConnected.waitOne();
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
	
	void resurrectPrimitive(ZkSyncPrimitive primitive) {
		if (!shutdown) {
			synchronized (resurrectList) {
				if (primitive.zooKeeper != this.zooKeeper) {
					// We already have a new ZooKeeper client the primitive can use to resurrect itself.
					primitive.zooKeeper = this.zooKeeper;
					primitive.resynchronize();
				} else {
					// Save a reference to the primitive for later resurrection.
					resurrectList.add(primitive);
				}
			}
		}
	}
	
	@Override
	public void process(WatchedEvent event)  {
		if (event.getType() == Event.EventType.None) {
    		KeeperState state = event.getState();
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
	 * performed. We notify our sync objects. 
	 */
	private void onConnected() {
		isConnected.set();
		processResurrectList();
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
	 * The ZooKeeper session has expired. Initiate the creation of a new client session, and notify our
	 * sync objects that the session is being reset.
	 */
	private void onSessionExpired() {
		isConnected.reset();
		connectExecutor.submit(clientCreator);
	}	
	
	/**
	 * Resurrect those primitives that can resynchronize themselves after session expiry. These are typically
	 * objects that would rather display the last best known state than no state. For example, a list of
	 * servers cooperating in a cluster.
	 */
	private void processResurrectList() {
		synchronized (resurrectList) {
			ZkSyncPrimitive[] toResurrect = resurrectList.toArray(new ZkSyncPrimitive[] {});
			resurrectList.clear(); // clear before resynchronizing to prevent reentrancy issues
			for (ZkSyncPrimitive primitive : toResurrect) {
				primitive.zooKeeper = this.zooKeeper;
				primitive.resynchronize();
			}
		}
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
					System.err.println("ZkSessionManager failed to connect client across network to specified cluster...");
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
	
	public static void initializeInstance(String connectString) throws InterruptedException, IOException, ExecutionException {
		instance = new ZkSessionManager(connectString);
	}	
	
	public static void initializeInstance(String connectString, int sessionTimeout, int maxAttempts) throws InterruptedException, IOException, ExecutionException {
		instance = new ZkSessionManager(connectString, sessionTimeout, maxAttempts);
	}
}
