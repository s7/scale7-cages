package org.scale7;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.scale7.concurrency.ManualResetEvent;
import org.scale7.zookeeper.cages.ZkContributedKeySet;
import org.scale7.zookeeper.cages.ZkLockBase;
import org.scale7.zookeeper.cages.ZkPath;
import org.scale7.zookeeper.cages.ZkReadLock;
import org.scale7.zookeeper.cages.ZkSessionManager;
import org.scale7.zookeeper.cages.ZkWriteLock;

public class AppTest extends AbstractIntegrationTest
{
    @Override
	protected void setUp() throws Exception {
		super.setUp();
        ZkSessionManager.initializeInstance(zkAddress + ":" + zkPort + "/UnitTests", 1000, Integer.MAX_VALUE);
	}

	@Override
	protected void tearDown() throws Exception {
		assertNotNull(ZkSessionManager.instance());
		ZkSessionManager.instance().shutdown();
		super.tearDown();
	}

    public void testCreateReadLocksInRoot() throws Exception {

    	createLocksInFolder(ZkReadLock.class, "/");
    }

	public void testCreateReadLocksInFolder() throws Exception {

		createLocksInFolder(ZkReadLock.class, "/" + (new Random()).nextInt() + "/");
    }

    public void testCreateWriteLocksInRoot() throws Exception {

    	createLocksInFolder(ZkWriteLock.class, "/");
    }

    public void testCreateWriteLocksInFolder() throws Exception {

    	createLocksInFolder(ZkWriteLock.class, "/" + (new Random()).nextInt() + "/");
    }

    private void createLocksInFolder(Class<? extends ZkLockBase> lockClass, String folder) throws Exception {

    	String rootLockPath = folder + (new Random()).nextInt();

    	// Acquire this lock path for first time
    	ZkLockBase lock = lockClass.getConstructor(String.class).newInstance(rootLockPath);
    	lock.acquire();
    	lock.release();

    	// Acquire this lock path for second time
    	// Because of way Zk works the root ZNode is persistent so need to test second creation
    	lock = lockClass.getConstructor(String.class).newInstance(rootLockPath);
    	lock.acquire();
    	lock.release();
    }

    public void testCreateContributedKeySet() throws Exception {

    	// Need to make sure path of set exists
    	ZkPath setPath = new ZkPath("/ClusterMembers");
    	setPath.waitSynchronized();

    	// Now can add members to set
    	ZkContributedKeySet ckSet1 = new ZkContributedKeySet("/ClusterMembers", new String[] {"myNodeId1"}, true);
    	ZkContributedKeySet ckSet2 = new ZkContributedKeySet("/ClusterMembers", new String[] {"myNodeId2"}, true);
    	ckSet1.waitSynchronized();
    	ckSet2.waitSynchronized();

    	assertTrue(ckSet1.getKeySet().equals(ckSet2.getKeySet()));
    	assertTrue(ckSet1.getKeySet().size() == 2);
    	assertTrue(ckSet1.getKeySet().contains("myNodeId1"));
    	assertTrue(ckSet1.getKeySet().contains("myNodeId2"));

    	// Add listener for set updates
    	final ManualResetEvent updated = new ManualResetEvent(false);
    	Runnable updateCallback = new Runnable() {
			@Override
			public void run() {
				updated.set();
			}
		};
		ckSet1.addUpdateListener(updateCallback, false);
		// Update set contribution
    	ckSet1.adjustMyContribution(new String[] {});

    	assertTrue(updated.waitOne(5000, TimeUnit.MILLISECONDS));
    	assertTrue(ckSet1.getKeySet().size() == 1);
    }
}
