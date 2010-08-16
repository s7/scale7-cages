package org.scale7.zookeeper.cages;

public class ZkValue extends ZkSyncPrimitive {

	ZkValue() throws InterruptedException {
		super(ZkSessionManager.instance());
	}

}
