package org.wyki.zookeeper.cages;

import org.apache.zookeeper.KeeperException;

@SuppressWarnings("serial")
public class ZkCagesException extends Exception {
	
	public enum Error {
		ZOOKEEPER_EXCEPTION,
		INTERRUPTED_EXCEPTION,
		LOCK_ALREADY_WAITING,
		LOCK_ALREADY_ABANDONED,
		LOCK_ALREADY_ACQUIRED,
		LOCK_ALREADY_RELEASED,
		LOCK_RELEASED_WHILE_WAITING,
		UNKNOWN_ERROR,
	}
	
	private Error error;
	private KeeperException keeperException;

	public ZkCagesException(KeeperException keeperException) {
		super();
		error = Error.ZOOKEEPER_EXCEPTION;
		this.keeperException = keeperException;
	}
	
	public ZkCagesException(Error error) {
		this.error = error;
	}
	
	public Error getErrorCode() {
		return error;
	}
	
	public KeeperException getKeeperException() {
		return keeperException;
	}
}
