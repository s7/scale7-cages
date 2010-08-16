package org.wyki.zookeeper.cages;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

/**
 * Base class for single path read and write locks.
 * 
 * TODO implement timeouts as basic means to rollback from deadlock. 
 * 
 * @author dominicwilliams
 *
 */
public abstract class ZkLockBase extends ZkSyncPrimitive implements ISinglePathLock {

	private String lockPath;
	private Object context;
	private ZkPath zkPath;
	private String thisNodeId;
	private String blockingNodeId;
	private ILockListener listener;
	private boolean tryAcquireOnly;
	private volatile LockState lockState;
	private Integer mutex;
	
	public ZkLockBase(String lockPath) {
		super(ZkSessionManager.instance());
		PathUtils.validatePath(lockPath);
		lockState = LockState.Idle;
		this.lockPath = lockPath;
		mutex = new Integer(-1);
	}

	/** {@inheritDoc} */
	@Override
	public void acquire() throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		createRootPath(lockPath);
		waitSynchronized();
	}
	
	/** {@inheritDoc} */
	@Override
	public void acquire(ILockListener listener, Object context) throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		this.listener = listener;
		this.context = context;
		addUpdateListener(reportStateUpdatedToListener, false);
		addDieListener(reportDieToListener);
		createRootPath(lockPath);
	}
	
	/** {@inheritDoc} */
	@Override
	public boolean tryAcquire() throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		tryAcquireOnly = true;
		createRootPath(lockPath);
		waitSynchronized();
		return lockState == LockState.Acquired;
	}
	
	/** {@inheritDoc} */
	@Override
	public void tryAcquire(ITryLockListener listener, Object context) throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		this.listener = listener;
		this.context = context;
		tryAcquireOnly = true;
		addUpdateListener(reportStateUpdatedToListener, false);
		addDieListener(reportDieToListener);
		createRootPath(lockPath);
	}
	
	/** {@inheritDoc} */
	@Override
	public void release() {
		safeLockState(LockState.Released);
	}
	
	/** {@inheritDoc} */
	@Override
	public LockState getState()
	{
		return lockState;
	}

	/**
	 * What path does this instance lock?
	 * @return								The path that has/will be locked by this lock instance
	 */
	@Override
	public String getLockPath() {
		return zkPath.getPath();
	}
	
	private void createRootPath(String path) throws InterruptedException {
		zkPath = new ZkPath(path, CreateMode.PERSISTENT);
		// TODO for now only persistent ZK nodes can have children. fix this.
		zkPath.addUpdateListener(createLockNode, true);
		zkPath.addDieListener(onLockPathError);		
	}
			
	private Runnable onLockPathError = new Runnable () {

		@Override
		public void run() {
			// Set the lock state
			safeLockState(LockState.Error);
			// Bubble the killer exception and die!
			die(zkPath.getKillerException());
		}
		
	};
	
	@Override
	protected void onDie(ZkCagesException killerException) {
		// We just set the lock state. The killer exception has already been set by base class
		safeLockState(LockState.Error);
	}
	
	private Runnable createLockNode = new Runnable () {

		@Override
		public void run() {
			zooKeeper().create(zkPath.getPath() + "/" + getType() + "-", new byte[0],
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, createLockNodeHandler, this);
		}
		
	};
		
	private StringCallback createLockNodeHandler = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			
			if (rc == Code.OK.intValue())
				thisNodeId = ZkLockNode.getLockNodeIdFromName(name);
			if (passOrTryRepeat(rc, new Code[] { Code.OK }, (Runnable)ctx))
				getQueuedLocks.run();
		}
		
	};	
	
	private Runnable getQueuedLocks = new Runnable () {

		@Override
		public void run() {
			zooKeeper().getChildren(zkPath.getPath(), null, queuedLocksHandler, this);
		}
		
	};
	
	private ChildrenCallback queuedLocksHandler = new ChildrenCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			// Upon successful enumeration of lock nodes, see if any are blocking this...
			if (passOrTryRepeat(rc, new Code[] { Code.OK }, (Runnable)ctx)) {
				// Create sorted list of nodes
				SortedSet<ZkLockNode> nodeQueue = new TreeSet<ZkLockNode>();
				for (String lockId : children)
					nodeQueue.add(ZkLockNode.lockNodeFromId(lockId, ZkLockBase.this.thisNodeId));
				// See who is blocking this
				ZkLockNode self = null;
				ZkLockNode prevNode = null;
				ZkLockNode prevWriteNode = null;
				Iterator<ZkLockNode> i = nodeQueue.iterator();
				while (i.hasNext()) {
					ZkLockNode node = i.next();
					if (node.self) {
						self = node;
						break;
					}
					prevNode = node;
					if (prevNode.type == LockType.Write)
						prevWriteNode = prevNode;
				}
				ZkLockNode blockingNode = null;
				if (self.type == LockType.Read)
					blockingNode = prevWriteNode;
				else
					blockingNode = prevNode;
				// Are we blocked?
				if (blockingNode != null) {
					blockingNodeId = blockingNode.name;
					// Should we give up, or wait?
					if (tryAcquireOnly) {
						// We abandon attempt to acquire
						safeLockState(LockState.Abandoned);
					} else
						// wait for blocking node
						watchBlockingNode.run();
				} else {
					// We are acquired!
					safeLockState(LockState.Acquired);
				}
			}
		}
		
	};	
	
	private Runnable watchBlockingNode = new Runnable() {

		@Override
		public void run() {
			zooKeeper().exists(zkPath.getPath() + "/" + blockingNodeId, ZkLockBase.this, blockingNodeHandler, this);
		}
		
	};
	
	private StatCallback blockingNodeHandler = new StatCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			if (rc == Code.NONODE.intValue())
				getQueuedLocks.run();
			else
				passOrTryRepeat(rc, new Code[] { Code.OK }, (Runnable)ctx);
		}
		
	};
	
	@Override
	protected void onNodeDeleted(String path) {
		getQueuedLocks.run();
	}
	
	private Runnable releaseLock = new Runnable () {

		@Override
		public void run() {
			zooKeeper().delete(zkPath.getPath() + "/" + thisNodeId, -1, releaseLockHandler, this);
		}
		
	};

	private VoidCallback releaseLockHandler = new VoidCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx) {
			passOrTryRepeat(rc, new Code[] { Code.OK, Code.NONODE }, (Runnable)ctx); 
		}
		
	};
	
	private Runnable reportStateUpdatedToListener = new Runnable() {

		@Override
		public void run() {
			if (tryAcquireOnly && lockState != LockState.Acquired) {
				// We know that an error has not occurred, because that is passed to handler below. So report attempt
				// to acquire locked failed because was already held.
				ITryLockListener listener = (ITryLockListener)ZkLockBase.this.listener;
				listener.onTryAcquireLockFailed(ZkLockBase.this, context);
			}
			else
				listener.onLockAcquired(ZkLockBase.this, context);
		}
		
	};
	
	private Runnable reportDieToListener = new Runnable() {

		@Override
		public void run() {
			listener.onLockError(getKillerException(), ZkLockBase.this, context);
		}
		
	};
		
	/**
	 * Set the lock state when we know an exception can't be thrown
	 * @param newState					The new lock state
	 */
	private void safeLockState(LockState newState) {
		try {
			setLockState(newState);
		} catch (ZkCagesException e) {
			e.printStackTrace();
			assert false : "Unknown condition";
		}
	}
	
	/**
	 * Set the lock state
	 * @param newState					The new lock state
	 * @throws ZkCagesException
	 */
	private void setLockState(LockState newState) throws ZkCagesException {
		
		synchronized (mutex) {
			switch (newState) {
			
			case Idle:
				assert false : "Unknown condition";
				
			case Waiting:
				/**
				 * We only set this state from the public interface methods. This means we can directly throw an 
				 * exception back at the caller!
				 */
				switch (lockState) {
				case Idle:
					// Caller is starting operation
					lockState = newState;
					return;
				case Waiting:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_WAITING);
				case Abandoned:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_ABANDONED);
				case Acquired:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_ACQUIRED);
				case Released:
					throw new ZkCagesException(ZkCagesException.Error.LOCK_ALREADY_RELEASED);
				default:
					assert false : "Unknown condition";
				}
				break;
				
			case Abandoned:
				/**
				 * We tried to acquire a lock, but it was already held and we are abandoning our attempt to acquire.
				 */
				switch (lockState) {
				case Waiting:
					// Attempt to acquire lock without blocking has failed
					lockState = newState;
					// Release our lock node immediately
					releaseLock.run();
					// Notify listeners about result
					if (listener != null) {
						ITryLockListener listener = (ITryLockListener)this.listener;
						listener.onTryAcquireLockFailed(this, context);
					}
					// Notify waiting callers about result
					onStateUpdated();
					return;
				case Released:
					// Logically the lock has already been released. No node was created, so no need to releaseLock.run()
					return;
				default:
					assert false : "Unknown condition";
				}
				break;
				
			case Acquired:
				/**
				 * We have successfully acquired the lock.
				 */
				switch (lockState) {
				case Waiting:
					// Attempt to acquire lock has succeeded
					lockState = newState;
					// Notify caller
					onStateUpdated();
					return;
				case Released:
				case Error:
					// The lock has already been logically released or an error occurred. We initiate node release, and return
					releaseLock.run();
					return;
				default:
					assert false : "Unknown condition";
				}
				break;
				
			case Released:
				/**
				 * We are releasing a lock. This can be done before a lock has been acquired if an operation is in progress.
				 */
				switch (lockState) {
				case Idle:
					// Change to the released state to prevent this lock being used again
					lockState = newState;
					return;
				case Released:
				case Abandoned:
					// We consider that release() has been called vacuously
					return;
				case Waiting:
					// release() method called while waiting to acquire lock (or during the process of trying to acquire a lock).
					// This causes an error!
					die(new ZkCagesException(ZkCagesException.Error.LOCK_RELEASED_WHILE_WAITING)); // die callback will set state
					return;
				case Acquired:
					// We are simply releasing the lock while holding it. This is fine!
					lockState = newState;
					// Initiate the release procedure immediately
					releaseLock.run();
					return;
				default:
					assert false : "Unknown condition";
				}
				break;
				
			case Error:
				switch (lockState) {
				case Released:
					// Error is vacuous now. Lock has already been released (or else, break in session will cause ephemeral node to disappear etc)
					return;
				default:
					// ZkSyncPrimitive infrastructure is handling passing exception notification to caller, so just set state
					lockState = newState;
					return;
				}
			}
			
			assert false : "Unknown condition";
		}
	}
}
