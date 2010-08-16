package org.wyki.zookeeper.cages;

/**
 * This class has two methods which are call
 * back methods when a lock is acquired and 
 * when the lock is released.
 *
 */
public interface ILockListener {

	/**
	 * A lock has been successfully acquired.
	 * 
     * @param lock						The lock that has been acquired
     * @param context					The context object passed to the lock's constructor
	 */
    public void onLockAcquired(ILock lock, Object context);
    
    /**
     * An error has occurred while waiting for the lock.
     * 
     * @param error						The error that has occurred
     * @param lock						The lock that has experienced the error
     * @param context					The context object passed to the lock's constructor
     */
    public void onLockError(ZkCagesException error, ILock lock, Object context);
}