package org.wyki.zookeeper.cages;

public class ZkLockNode implements Comparable<ZkLockNode> {

	public final String name;
	public final ILock.LockType type;
	public final int seqNo;
	public final boolean self;
	
	/**
	 * Retries the lock node id from a full lock node name path (the path after addition of sequence number).
	 * 
	 * @param path						The name path to retrieve the id from
	 * @return							The lock node id
	 */
	static String getLockNodeIdFromName(String lockNodeName) {
		int lastPathSep = lockNodeName.lastIndexOf("/");
		if (lastPathSep != -1)
			return lockNodeName.substring(lastPathSep+1);
		else
			return lockNodeName;
	}

	/**
	 * Construct a lock node from a lock id. This can then be used in lock wait algorithms.
	 * 
	 * @param lockId					The id of the lock node e.g. write-0000019
	 * @param lockIdSelf				The id of the lock performing the processing
	 * @return							A lock node wrapper object
	 */
	static ZkLockNode lockNodeFromId(String lockId, String lockIdSelf) {
		
		ILock.LockType type;
		
		if (lockId.startsWith(ILock.LockType.Read.toString()))
			type = ILock.LockType.Read;
		else if (lockId.startsWith(ILock.LockType.Write.toString()))
			type = ILock.LockType.Write;
		else
			return null; // not lock node
		
		int seqNo;
		int sepIdx = lockId.lastIndexOf("-");
		if (sepIdx == -1)
			return null; // not lock node
		
		try {
			seqNo = Integer.parseInt(lockId.substring(sepIdx + 1));
		} catch (Exception ex) {
			return null; // not lock node
		}
		
		return new ZkLockNode(lockId, type, seqNo, lockId.equals(lockIdSelf));
	}
	
	private ZkLockNode(String name, ILock.LockType type, int seqNo, boolean self) {
		this.name = name;
		this.type = type;
		this.seqNo = seqNo;
		this.self = self;
	}
	
	/**
	 * The comparison function is designed so that any lock we need to wait for is sorted below us
	 * in the lock node queue.
	 */
	@Override
	public int compareTo(ZkLockNode o) {
		
		if (seqNo < o.seqNo)
			return -1;
		
		return 1;
	}

}
