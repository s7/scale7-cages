package org.wyki.zookeeper.cages;

public interface ILock {

	/**
	 * Try to acquire the lock. Block until the lock is acquired, or an error occurs. An exception is
	 * thrown if an error occurs.
	 * 
	 * @throws ZkCagesException
	 * @throws InterruptedException
	 */
	void acquire() throws ZkCagesException, InterruptedException;
	
	/**
	 * Asynchronously try to acquire the lock. The listener object receives the result.
	 * 
	 * @param listener
	 * @throws InterruptedException
	 */
	void acquire(ILockListener listener, Object context) throws ZkCagesException, InterruptedException;
	
	/**
	 * Try to acquire the lock in a synchronous manner, but return without waiting if the lock is already held
	 * by another instance. Note this method is not asynchronous because blocking network IO can be performed
	 * to determine whether a lock is already held by another instance.
	 * 
	 * @return								Whether has lock has been acquired
	 * @throws InterruptedException
	 * @throws ZkCagesException
	 */
	boolean tryAcquire() throws ZkCagesException, InterruptedException;
	
	/**
	 * Try to acquire the lock in an asynchronous manner. If the lock is already held, the operation gives up
	 * without waiting. This method does not wait for any blocking IO and the result is returned through the
	 * provided listener interface
	 * 
	 * @param listener						The listener object to be notified of the result
	 * @throws InterruptedException 
	 */
	void tryAcquire(ITryLockListener listener, Object context) throws ZkCagesException, InterruptedException;
	
	/**
	 * Release the lock.
	 */
	void release();
	
	enum LockState {
		Idle,
		Waiting,
		Abandoned,
		Acquired,
		Released,
		Error
	}
	
	/**
	 * Determines the state of the lock.
	 * 
	 * @return							The lock state
	 */
	LockState getState();
	
	enum LockType {
		Read,
		Write,
		Multi
	}
	
	/**
	 * Determines the type of a lock
	 * 
	 * @return 							The lock type
	 */
	LockType getType();
}
