package org.scale7.zookeeper.cages;

public interface IMultiPathLock extends ILock {

	/**
	 * Get all the read lock paths the lock will acquire
	 *
	 * @return							The read lock paths
	 */
	String[] getReadLockPaths();

	/**
	 * Get all the write lock paths the lock will acquire
	 *
	 * @return							The write lock paths
	 */
	String[] getWriteLockPaths();

	/**
	 * Discover whether the multilock is or will lock a specific path
	 *
	 * @param lockPath						The lock path to query
	 * @return								The type of lock on that path. LockType.None if no lock for path
	 */
	LockType contains(String lockPath);
}
