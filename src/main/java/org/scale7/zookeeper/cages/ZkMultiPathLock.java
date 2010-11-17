package org.scale7.zookeeper.cages;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.scale7.networking.utility.NetworkAlgorithms;

/**
 * A class that acquires a set of read and write locks all together, or not at all. The purpose of
 * this class is to prevent the introduction of complex deadlock scenarios in distributed programming
 * environments. Whenever two or more single path locks are acquired sequentially, the possibility of
 * deadlock arises (for example if process A requests r(X) and then requests w(Y), but process B already
 * holds r(Y) and then simultaneously requests w(X)). Detecting closed wait graphs in a distributed
 * environment is difficult and expensive. One solution is for developers to acquire all single
 * lock paths with a timeout, such that in a deadlock situation an exception will be thrown, and hopefully
 * all held locks released as the operation is rolled back. However, there are a number of problems with
 * this approach: (1) developers must be relied upon to set appropriate timeouts (2) developers may
 * have to roll back partially completed operations when an a timeout exception is thrown when they
 * are trying to acquire a nested lock (3) even if developers have handled this complexity
 * correctly, in such situations where deadlock occurs during the period leading up to the timeout
 * many operations and locks may have become queued up, thus leading to a stampede of lock acquisition
 * attempts and a likely repeat of the deadlock occurring. For this reason, it is better to forbid
 * developers from acquiring single lock paths in a nested manner, and to require them to use this
 * q multi-lock system where all locks required for an operation are acquired and released together,
 * thus avoiding the risk of deadlock occurring.
 *
 * @author dominicwilliams
 *
 */
public class ZkMultiPathLock implements IMultiPathLock {

	private final int MIN_RETRY_DELAY = 125;
	private final int MAX_RETRY_DELAY = 8000;

	private ArrayList<ISinglePathLock> locks;
	private ISinglePathLock[] sortedLocks;
	private volatile LockState lockState;
	private final Integer mutex;

	public ZkMultiPathLock() {
		lockState = LockState.Idle;
		locks = new ArrayList<ISinglePathLock>(32);
		mutex = new Integer(-1);
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
		setLockState(LockState.Waiting);
		prepareSortedLockArray();
		int attempts = 0;
		while (true) {
			attempts++;
			if (doTryAcquire()) {
				setLockState(LockState.Acquired);
				return;
			}
			Thread.sleep(NetworkAlgorithms.getBinaryBackoffDelay(attempts, MIN_RETRY_DELAY, MAX_RETRY_DELAY));
		}
	}

	/** {@inheritDoc} */
	@Override
	public void acquire(ILockListener listener, Object context) throws ZkCagesException, InterruptedException {
		throw new NotImplementedException();
	}

	/** {@inheritDoc} */
	@Override
	public boolean tryAcquire() throws ZkCagesException, InterruptedException {
		setLockState(LockState.Waiting);
		prepareSortedLockArray();
		if (doTryAcquire()) {
			setLockState(LockState.Acquired);
			return true;
		}
		setLockState(LockState.Abandoned);
		return false;
	}

	/**
	 * When locks are being acquired sequentially, this will produce the earliest possible back off when
	 * there is contention (since multi-locks will try to acquire the same paths first)
	 */
	protected void prepareSortedLockArray() {
		sortedLocks = locks.toArray(new ISinglePathLock[0]);
		Arrays.sort(sortedLocks);
	}

	protected boolean doTryAcquire() throws ZkCagesException, InterruptedException {
		// loop until we acquire all locks
		try {
			boolean tryFailed = false;
			// try and get them all
			for (ILock lock : sortedLocks)
				if (!lock.tryAcquire()) {
					tryFailed = true;
					break;
				}
			// done if successful
			if (!tryFailed) {
				return true;
			}
			// otherwise roll back
			release();
			return false;
		} catch (ZkCagesException ex) {
			// record killer ZooKeeper exception
			setLockState(LockState.Error);
			// roll back any locks we have obtained so far
			release();
			// re-throw exception
			throw ex;
		} catch (InterruptedException ex) {
			// roll back any locks we have obtained so far
			release();
			// re-throw interrupted
			Thread.currentThread().interrupt();
			throw ex;
		}
	}

	/** {@inheritDoc} */
	@Override
	public void tryAcquire(ITryLockListener listener, Object context) throws ZkCagesException, InterruptedException {
		throw new NotImplementedException();
	}

	/** {@inheritDoc} */
	@Override
	public void release()  {
		synchronized (mutex) {
			for (ILock lock : sortedLocks)
				if (lock.getState() == LockState.Acquired)
					lock.release();
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
					throw new ZkCagesException(ZkCagesException.Error.LOCK_RELEASED_WHILE_WAITING);
				case Acquired:
					// We are simply releasing the lock while holding it. Everything fine.
					lockState = newState;
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
					return;
				}
			}

			assert false : "Unknown condition";
		}
	}
}
