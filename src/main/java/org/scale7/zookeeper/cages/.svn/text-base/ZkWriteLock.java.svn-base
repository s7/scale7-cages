package org.wyki.zookeeper.cages;

public class ZkWriteLock extends ZkLockBase {
	
	public ZkWriteLock(String lockPath) {
		super(lockPath);
	}

	/** {@inheritDoc} */
	@Override
	public LockType getType() {
		return LockType.Write;
	}
}
