package org.scale7.zookeeper.cages;

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
