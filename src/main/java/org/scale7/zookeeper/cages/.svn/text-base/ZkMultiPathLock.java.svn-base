package org.wyki.zookeeper.cages;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.wyki.concurrency.ManualResetEvent;
import org.wyki.networking.utility.NetworkAlgorithms;

/**
 * A class that acquires a set of read and write locks all together, or not at all. The purpose of
 * this class is to prevent the introduction of complex deadlock scenarios in distributed programming
 * environments. Whenever two or more single path locks are acquired sequentially, the possibility of
 * deadlock arises (for example if process A requests r(X) and then requests w(Y), but process B already
 * holds r(Y) and then simultaneously requests w(X)). Detecting closed wait graphs in a distributed
 * environment is necessarily difficult and expensive. One solution is for developers to acquire all single
 * lock paths with a timeout, such that in a deadlock situation an exception will be thrown, and hopefully
 * all held locks released as the operation is rolled back. However, there are a number of problems with
 * this approach: (1) developers must be relied upon to set appropriate timeouts (2) developers may 
 * have to roll back partially completed operations when an a timeout exception is thrown when they
 * are trying to acquire a nested lock (3) even if developers have handled the aforementioned complexity
 * correctly, in such situations where deadlock occurs during the period leading up to the timeout
 * many operations and locks may have become queued up, thus leading to a stampede of lock acquisition
 * attempts and a likely repeat of the deadlock occurring. For this reason, it is better to forbid
 * developers from acquiring single lock paths in a nested manner, and to require them to use this 
 * multi-lock paradigm, where all locks required for an operation are acquired and released together,
 * thus avoiding the risk of deadlock occurring. It is even arguable, that the usage of the single
 * path read and write lock classes should be forbidden altogether in projects pursuing maximum robustness,
 * since this class will be only be slightly less efficient unless the lock paths involved are heavily
 * contested, and in practice - since deadlocks will occur - it will be much more efficient. 
 * 
 * @author dominicwilliams
 *
 */
public class ZkMultiPathLock implements IMultiPathLock, ITryLockListener {
	
	private final int MIN_RETRY_DELAY = 125;
	private final int MAX_RETRY_DELAY = 8000;
	
	private ILockListener listener;
	private boolean tryAcquireOnly;
	private volatile ArrayList<ISinglePathLock> locks;
	private final ManualResetEvent isDone;
	private ZkCagesException killerException;
	private volatile LockState lockState;
	private ScheduledExecutorService retry;
	private Object context;
	private int attemptId;
	private final Integer mutex;
	
	public ZkMultiPathLock() {
		lockState = LockState.Idle;
		locks = new ArrayList<ISinglePathLock>(32);
		isDone = new ManualResetEvent(false);
		retry = ZkSessionManager.instance().callbackExecutor;
		mutex = new Integer(-1);
		attemptId = 0;
	}
	
	/**
	 * Add a write lock requirement to the multi-lock
	 * 
	 * @param lockPath						The read lock path to add
	 * @throws InterruptedException
	 */
	public void addReadLock(String lockPath) {
		locks.add(new ZkReadLock(lockPath));
	}
	
	/**
	 * Add a write lock requirement to the multi-lock
	 * 
	 * @param lockPath						The write lock path to add
	 * @throws InterruptedException
	 */
	public void addWriteLock(String lockPath) {
		locks.add(new ZkWriteLock(lockPath));
	}
	
	/** {@inheritDoc} */
	@Override
	public void acquire() throws ZkCagesException, InterruptedException {
		synchronized (mutex) {
			setLockState(LockState.Waiting, null);
			tryAcquireOnly = false;
			tryAcquireAll();
		}
		isDone.waitOne();
		if (killerException != null)
			throw killerException;
	}
	
	/** {@inheritDoc} */
	@Override
	public void acquire(ILockListener listener, Object context) throws ZkCagesException, InterruptedException {
		synchronized (mutex) { 
			setLockState(LockState.Waiting, null);
			tryAcquireOnly = false;
			this.listener = listener;
			this.context = context;
			tryAcquireAll();
		}
	}
	
	/** {@inheritDoc} */
	@Override
	public boolean tryAcquire() throws ZkCagesException, InterruptedException {
		synchronized (mutex) {
			setLockState(LockState.Waiting, null);
			tryAcquireOnly = true;
			tryAcquireAll();
		}
		isDone.waitOne();
		if (killerException != null)
			throw killerException;
		return lockState == LockState.Acquired;
	}
	
	/** {@inheritDoc} */
	@Override
	public void tryAcquire(ITryLockListener listener, Object context) throws ZkCagesException, InterruptedException {
		synchronized (mutex) {
			setLockState(LockState.Waiting, null);
			tryAcquireOnly = true;
			this.listener = listener;
			this.context = context;
			tryAcquireAll();
		}
	}
	
	/** {@inheritDoc} */
	@Override
	public void release()  {
		synchronized (mutex) {
			safeLockState(LockState.Released, null);
		}
	}
	
	/** {@inheritDoc} */
	@Override
	public LockState getState()
	{
		return lockState;
	}
	
	/** {@inheritDoc} */
	@Override
	public LockType getType() {
		return LockType.Multi;
	}
	
	/** {@inheritDoc} */
	@Override
	public String[] getReadLockPaths() {
		return getLockPathsByType(ILock.LockType.Read);
	}

	/** {@inheritDoc} */
	@Override
	public String[] getWriteLockPaths() {
		return getLockPathsByType(ILock.LockType.Write);
	}
	
	private String[] getLockPathsByType(ILock.LockType type) {
		ArrayList<String> lockPaths = new ArrayList<String>(32);
		List<ISinglePathLock> lockList = locks;
		for (ISinglePathLock lock : lockList)
			if (lock.getType() == type)
				lockPaths.add(lock.getLockPath());
		return lockPaths.toArray(new String[] {});
	}

	/** {@inheritDoc} */
	@Override
	public void onTryAcquireLockFailed(ILock lock, Object context) {
		synchronized (mutex) {
			if (oldAttemptFeedback(context))
				return;
			if (tryAcquireOnly) {
				safeLockState(LockState.Abandoned, null);
			} else {
				// Schedule new attempt to acquire locks over the paths
				rescheduleTryAcquireAll(context);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public void onLockAcquired(ILock lock, Object context) {
		synchronized (mutex) {
			if (oldAttemptFeedback(context))
				return;
			for (ILock currLock : locks) {
				if (currLock.getState() != LockState.Acquired)
					return;
			}
			safeLockState(LockState.Acquired, null);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void onLockError(ZkCagesException error, ILock lock, Object context) {
		synchronized (mutex) {
			if (oldAttemptFeedback(context))
				return;
			safeLockState(LockState.Error, error);
		}
	}
	
	private void tryAcquireAll() throws ZkCagesException, InterruptedException {
		Object context = new Integer(attemptId);
		if (locks.size() == 1)
			// Only 1 lock is being acquired. Therefore we can simply wait to acquire a lock over its path
			// without creating an opportunity for deadlock. This is more efficient where the path is contended
			// than repeatedly trying to acquire paths according to a BEB schedule.
			locks.get(0).acquire(this, context);
		else
			// Try to acquire all the locks together. Do not block waiting for any individual path. If we cannot
			// acquire all paths together, we wil retry according to the BEB schedule.
			for (ILock lock : locks)
				lock.tryAcquire(this, context);
	}
	
	private void releaseAll() {
		for (ILock lock : locks)
			lock.release();
	}
	
	private boolean oldAttemptFeedback(Object context) {
		int attemptId = (Integer)context;
		return this.attemptId != attemptId;
	}
		
	private void rescheduleTryAcquireAll(Object context) {
		// Increment attemptId to begin a new attempt
		attemptId++;
		// Release all existing locks / attempts to lock
		releaseAll();
		// Create new array of lock objects
		ArrayList<ISinglePathLock> newLockObjs = new ArrayList<ISinglePathLock>(locks.size());
		for (ISinglePathLock lock : locks) {
			if (lock.getType() == LockType.Read)
				newLockObjs.add(new ZkReadLock(lock.getLockPath()));
			else if (lock.getType() == LockType.Write)
				newLockObjs.add(new ZkWriteLock(lock.getLockPath()));
			else
				assert false;
		}
		locks = newLockObjs;
		// Schedule new attempt
		retry.schedule(retryAcquireLocks, NetworkAlgorithms.getBinaryBackoffDelay(attemptId, MIN_RETRY_DELAY, MAX_RETRY_DELAY), TimeUnit.MILLISECONDS);
	}
	
	private Runnable retryAcquireLocks = new Runnable () {

		@Override
		public void run() {
			synchronized (mutex) {
				try {
					tryAcquireAll();
				} catch (ZkCagesException e) {
					safeLockState(LockState.Error, e);
				} catch (InterruptedException e) {
					safeLockState(LockState.Error, new ZkCagesException(ZkCagesException.Error.INTERRUPTED_EXCEPTION));
					Thread.currentThread().interrupt();
				}
			}
		}
		
	};
	
	/**
	 * Set the lock state when we know an exception can't be thrown
	 * @param newState					The new lock state
	 */
	private void safeLockState(LockState newState, ZkCagesException killerException) {
		try {
			setLockState(newState, killerException);
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
	private void setLockState(LockState newState, ZkCagesException killerException) throws ZkCagesException {
		
		switch (newState) {
		
		case Idle:
			assert false : "Unknown condition";
			
		case Waiting:
			/**
			 * We only set this state from the public interface methods. This means we can directly throw an 
			 * exception back at the caller.
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
				// Release our lock nodes immediately
				releaseAll();
				// Notify waiting caller about result
				isDone.set();
				// Notify listener about result
				if (listener != null) {
					ITryLockListener listener = (ITryLockListener)this.listener;
					listener.onTryAcquireLockFailed(this, context);
				}
				return;
			case Released:
			case Error:
				// The lock nodes have already been released. No need to releaseAll();
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
				isDone.set();
				// Notify listener
				if (listener != null) {
					listener.onLockAcquired(this, context);
				}
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
			case Waiting:
				setLockState(LockState.Error, new ZkCagesException(ZkCagesException.Error.LOCK_RELEASED_WHILE_WAITING));
				return;
			case Acquired:
				// We are simply releasing the lock while holding it. Everything fine.
				lockState = newState;
				// Initiate the release procedure immediately
				releaseAll();
			case Released:
			case Abandoned:
				// We consider that release() has been called vacuously
				return;
			default:
				assert false : "Unknown condition";
			}
			break;
			
		case Error:
			switch (lockState) {
			case Released:
			case Error:
				// Error is vacuous now. Locks have already been released.
				return;
			default:
				// An error has occurred.
				lockState = newState;
				// Record the killer exception
				this.killerException = killerException;
				// Initiate the release procedure immediately
				releaseAll();
				// Notify caller
				isDone.set();
				// Notify listener
				if (listener != null) {
					listener.onLockError(killerException, this, context);
				}
				return;
			}
		}
		
		assert false : "Unknown condition";
	}
}
