package org.wyki.zookeeper.cages;

public class ZkReadLock extends ZkLockBase {
	
	public ZkReadLock(String lockPath) {
		super(lockPath);
	}

	/** {@inheritDoc} */
	@Override
	public LockType getType() {
		return LockType.Read;
	}
}
