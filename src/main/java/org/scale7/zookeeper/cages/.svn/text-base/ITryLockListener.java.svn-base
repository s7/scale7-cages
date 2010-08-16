package org.wyki.zookeeper.cages;

public interface ITryLockListener extends ILockListener {

	/**
	 * The attempt to acquire a lock failed because it was already held
	 * 
	 * @param lock					The lock instance that failed to acquire
	 * @param context				The context object passed to the lock's constructor
	 */
	void onTryAcquireLockFailed(ILock lock, Object context);
	
}
