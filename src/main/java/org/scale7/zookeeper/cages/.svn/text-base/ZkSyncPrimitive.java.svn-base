package org.wyki.zookeeper.cages;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.wyki.concurrency.ManualResetEvent;

public abstract class ZkSyncPrimitive implements Watcher {
	
	/**
	 * The ZooKeeper session
	 */
	ZooKeeper zooKeeper;
	/**
	 * The parent/manager/context for the ZooKeeper session
	 */
	private ZkSessionManager session;
	/**
	 * Tasks to be run when the logical state of the primitive changes e.g. a lock acquired, a list gets new items
	 */
	private List<Runnable> stateUpdateListeners;
	/**
	 * Tasks to be run when the primitive enters into an unsynchronnized state
	 */
	private List<Runnable> dieListeners;
	/**
	 * Event that indicates that our state is synchronized and "ready" to use
	 */
	private ManualResetEvent isSynchronized;
	/**
	 * Indicates that re-synchronization is needed on connect. Only occurs in rare case we try to recover
	 * from session expiry e.g. when derived class prefers to offer stale data than offer no data. This is
	 * special case and for most synch primitives e.g. lock recovery has to happen application level.
	 */
	private volatile boolean resynchronizeNeeded;
	/**
	 * Exception indicating what killed this synchronization primitive
	 */
	private volatile ZkCagesException killedByException;
	/**
	 * Interrupted task in asynchronous operation sequence, which needs to be re-run on connect.
	 */
	private Runnable retryOnConnect;
	/**
	 * Number of attempts retrying a task interrupted by some error e.g. timeout
	 */
	private int retries;
	/**
	 * Mutex for use synchronizing access to private members
	 */
	private Integer mutex;
	
	protected ZkSyncPrimitive(ZkSessionManager session) {
    	this.session = session;
    	zooKeeper = this.session.zooKeeper;
    	stateUpdateListeners = null;
    	dieListeners = null;
    	isSynchronized = new ManualResetEvent(false);
    	resynchronizeNeeded = false;
    	retryOnConnect = null;
    	retries = 0;
    	mutex = new Integer(-1);
    }
    
    /**
     * Wait until the primitive has reached a synchronized state. If the operation was successful,
     * this is triggered when a derived class calls <code>onStateChanged()</code> for the first time. If the
     * operation was unsuccessful, the relevant exception is thrown.
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
	public void waitSynchronized() throws ZkCagesException, InterruptedException {
		isSynchronized.waitOne();
		
		if (getKillerException() == null)
			return;
		
		throw getKillerException();
	}    
    
    /**
     * Add a listener task to be executed when the object enters the synchronized state, and every time it updates its
     * state thereafter (as marked by derived classes calling <code>onStateUpdated()</code>).
     * 
     * @param handler 					The listener task to execute when the state has changed. A weak reference is taken.
     * @param doStartupRun 				If the state of the primitive is already synchronized then run the handler immediately
     */
    public void addUpdateListener(Runnable handler, boolean doStartupRun) {
    	synchronized (mutex) {
    		if (stateUpdateListeners == null) {
    			stateUpdateListeners = new ArrayList<Runnable>(8);
    		}
    		// Add to listener set first to avoid reentrancy race
    		stateUpdateListeners.add(handler);
    		// If we are already synchronized then trigger
    		if (doStartupRun && killedByException == null && isSynchronized.isSignalled()) {
    			handler.run();
    		}
    	}
    }	
    
    public void removeUpdateListener(Runnable handler) {
    	stateUpdateListeners.remove(handler);
    }
    
    public void addDieListener(Runnable handler) {
    	synchronized (mutex) {
    		if (dieListeners == null) {
    			dieListeners = new ArrayList<Runnable>(8);
    		}
    		// Add to listener set first to avoid reentrancy race
    		dieListeners.add(handler);
    		// If we are already synchronized then trigger
    		if (killedByException != null) {
    			handler.run();
    		}
    	}
    }	    
	
    /**
     * Returns whether the synchronization primitive is still valid / alive. 
     * @return 							Whether this primitive is alive and can be used
     */
    public boolean isAlive() {
    	return killedByException != null;
    }
    
    /**
     * If the primitive has been killed, returns the exception that has killed it.
     * @return							The exception that killed the primitive
     */
    public ZkCagesException getKillerException() {
    	return killedByException;
    }
    
    /**
     * Whether this primitive is attempting to resurrect itself after session expiry. 
     * @return 							Whether the primitive is resynchronizing 
     */
    public boolean isResynchronizing() {
    	return resynchronizeNeeded;
    }
        
	/**
	 * Must be called by derived classes when they have successfully updated their state.
	 */
	protected void onStateUpdated() {
		synchronized (mutex) {
			killedByException = null; 
			// Notify handlers ***before*** signalling state update to allow pre-processing
			if (stateUpdateListeners != null) {
				for (Runnable handler : stateUpdateListeners)
					handler.run();
			}
			// Signal state updated
			isSynchronized.set();
		}
	}	
    
    /**
     * If you have indicated that you wish to resurrect your synchronization primitive after a session expiry
     * or other event that would otherwise kill it - for example by returning <code>true</code> from
     * <code>shouldResurrectOnSessionExpiry()</code> - you need to override this method to perform the
     * re-synchronization steps. You might choose to ressurect/re-synchronize for example in a case where your 
     * primitive maintains a listing of nodes in a cluster, and you would rather maintain the last good known
     * record and try to re-synchronize rather than blow up in the case where for some reason a session is 
     * expired - for example after ZooKeeper has temporarily gone down or been partitioned. The ZooKeeper
     * documentation warns against libraries that attempt to re-synchronize, but it seems there are some cases
     * where it is valid to do so.
     */
	protected void resynchronize() {}
	
	/**
	 * Override to be notified of death event.
	 */
	protected void onDie(ZkCagesException killerException) {}
	
	/**
	 * Override to be notified of connection event.
	 */
	protected void onConnected() {}    
    
    /**
     * Override to be notified of disconnection event.
     */
	protected void onDisconnected() {}
    
    /**
     * Override to be notified of session expiry event.
     */
	protected void onSessionExpired() {}
    
    /**
     * Override to be notified of node creation event.
     * 
     * @param path 						The created path
     */
	protected void onNodeCreated(String path) {}
    
    /**
     * Override to be notified of node deletion event.
     * 
     * @param path 						The deleted path
     */
	protected void onNodeDeleted(String path) {}
    
    /**
     * Override to be notified of node data changing event.
     * 
     * @param path 						The path of the changed node
     */
	protected void onNodeDataChanged(String path) {}
    
    /**
     * Override to be notified of node children list changed event.
     * 
     * @param path 						The path of the parent node whose children have changed
     */
	protected void onNodeChildrenChanged(String path) {}
    
    /**
     * Override to indicate whether operations should be retried on error
     * 
     * @return 							Whether to retry
     */
	protected boolean shouldRetryOnError() { return false; }
    
    /**
     * Override to indicate whether operations should be retried on timeout error
     * 
     * @return 							Whether to retry
     */
	protected boolean shouldRetryOnTimeout() { return true; }
    
    /**
     * Override to indicate whether to resurrect the primitive and re-synchronize after session expiry.
     * See the comments for <code>resynchronize()</code> for discussions of rare cases where this is desirable.
     * Only do this with extreme caution. 
     * 
     * @return 							Whether to re-synchronize after session expiry
     */
	protected boolean shouldResurrectOnSessionExpiry() { return false; }    
	    
    ZooKeeper zooKeeper() {
    	return zooKeeper;
    }	
    
    /**
     * Permanently kill this synchronization primitive. It cannot be resurrected.
     * 
     * @param rc						The code of the ZooKeeper error that killed this primitive
     */
    protected void die(Code rc) {
    	KeeperException killerException = KeeperException.create(rc);
    	die(killerException);
    }      
    
    /**
     * Permanently kill this synchronization primitive. It cannot be resurrected. This method is typically called
     * by a derived class to pass an exception received from another ZkSyncPrimitive instance it has been using to 
     * implement its algorith
     * 
     * @param killerException			The killer exception.
     */
    protected void die(KeeperException killerException) {
    	die(new ZkCagesException(killerException));
    }
    
    protected void die(ZkCagesException killerException) {
    	synchronized (mutex) {
	    	// Record that we have been killed off by the exception passed by a derived class. This might have been generated
    		// by a contained ZkSyncPrimitive object we were using in the course of an algorithm
	    	this.killedByException = killerException;
	    	// Call into derived event handler
	    	onDie(killerException);
	    	// Notify listeners ***before*** signalling state update to allow pre-processing of error
	    	if (dieListeners != null) {
	    		for (Runnable handler : dieListeners)
					handler.run();
	    	}
	    	// Death is a synchronized state!
	    	isSynchronized.set();
    	}
    }
    
    /**
     * Prepares the next step in an asynchronous execution, based upon the return code from the previous step.
     * 
     * @param rc 						The ZooKeeper return code from the previous step
     * @param acceptable 				The acceptable list of return codes from the previous step
     * @param operation					The operation from the previous step (provided so it might be retried
     * @return 							Whether the next step should be started
     */
    protected boolean passOrTryRepeat(int rc, Code[] acceptable, Runnable operation) {
		
		Code opResult = Code.get(rc);
		
		for (Code code : acceptable) {
			if (opResult == code) {
				retries = 0;
				return true;
			}
		}

		switch (opResult) {
		case CONNECTIONLOSS:
			retryOnConnect(operation);
			break;
		case SESSIONMOVED:	// we assume that this is caused by request flowing over "old" connection. will be resolve with time. 
		case OPERATIONTIMEOUT:
			if (shouldRetryOnTimeout()) {
				retryAfterDelay(operation, retries++);
			}
			break;
		case SESSIONEXPIRED:
			if (shouldResurrectOnSessionExpiry()) {
				resynchronizeNeeded = true;
				requestRessurrection();
			} else {
				die(opResult);
			}
			break;
		default:
			if (shouldRetryOnError()) {
				retryAfterDelay(operation, retries++);
			} else {
				die(opResult);
			}
			break;
		}
		
		return false;
	}    
    
	@Override
	public void process(WatchedEvent event)  {
		String eventPath = event.getPath();
		EventType eventType = event.getType();
		KeeperState keeperState = event.getState();
		
		switch (eventType) {
		case None:
    		switch (keeperState) {
    		case SyncConnected:
    			if (resynchronizeNeeded) {
    				resynchronizeNeeded = false;
    				resynchronize();
    			} else {
    				onConnected();
    				if (retryOnConnect != null) {
    					retryOnConnect.run();
    					retryOnConnect = null; 
    				}
    			}
    			break;
    		case Disconnected:
    			onDisconnected();
    			break;
    		case Expired:
    			if (shouldResurrectOnSessionExpiry()) {
    				resynchronizeNeeded = true;
    				requestRessurrection();
    			} else {
    				die(Code.SESSIONEXPIRED);
    			}
    			onSessionExpired();
    			break;
    		}
			break;
		case NodeCreated:
			onNodeCreated(eventPath);
			break;
		case NodeDeleted:
			onNodeDeleted(eventPath);
			break;
		case NodeDataChanged:
			onNodeDataChanged(eventPath);
			break;
		case NodeChildrenChanged:
			onNodeChildrenChanged(eventPath);
        	break;
        default:
        	// in case version mismatch
        	die(Code.SYSTEMERROR);
        	break;
		}
	}    
                		
    private void retryOnConnect(Runnable operation) {
    	retryOnConnect = operation;
    }  
    
    private void retryAfterDelay(Runnable operation, int retries) {
    	session.retryPrimitiveOperation(operation, retries);
    }
    
    private void requestRessurrection() {
    	session.resurrectPrimitive(this);
    }
}
