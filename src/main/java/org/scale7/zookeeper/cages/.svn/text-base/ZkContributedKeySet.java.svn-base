package org.wyki.zookeeper.cages;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;

/**
 * A set identified by a path on ZooKeeper that comprises entries from all ZkContributedSet instances contributing to
 * that path. Each instance owns the entries it successfully contributes, and when it's ZooKeeper session is broken, or
 * when it is finalized, all the entries it has contributed to the set are removed from the set. This makes it useful
 * for tasks such as cluster node management.
 * @author dominicwilliams
 *
 */
public class ZkContributedKeySet extends ZkSyncPrimitive {
	/**
	 * The ZooKeeper path where the contributed key set resides
	 */
	private final String rootPath;
	/**
	 * The current values in the contributed key set
	 */
	private volatile Set<String> set;
	/**
	 * Whether the class should return the "last known" value of the contributed key set if the set becomes unsynchronized.
	 * This is valuable in situations where, for example, the ZooKeeper cluster is temporarily unreachable but where say
	 * the contributed keys represent the nodes in a server cluster and its desirable that the cluster members can still be
	 * reached.
	 */
	private final boolean allowDirty;
	/**
	 * Values that this instance successfully contributed to the distributed key set
	 */
	private final ConcurrentHashMap<String, Boolean> successfulContributions;
	/**
	 * Values that this instance failed to contribute to the distributed key set, for instance because another instance
	 * had already contributed them (each entry in a contributed key set is "owned" by exactly one instance).
	 */
	private final ConcurrentHashMap<String, Boolean> failedContributions;
	/**
	 * The values that this instance has attempts to contribute to the distributed key set.
	 */
	private String[] myContribution;
		
	/**
	 * Maintains the contents of a set to which each connected user contributes members. Note
	 * that in a contributed set, each user of the set owns the entries they have created. When such a 
	 * user disconnects from ZooKeeper, then all the entries they created will be automatically deleted. 
	 * This makes a contributed set useful for scenarios such as cluster node management.
	 * @param session			The ZooKeeper session manager
	 * @param path				The path uniquely identifying the distributed set
	 * @param myEntries			Entries that we wish to add to the set, while our ZooKeeper session is active
	 * @param allowDirty		Whether the set should try to recover from a "dead" state and continue showing "dirty" values while unsynchronized (session expired)
	 * @throws InterruptedException
	 */
	public ZkContributedKeySet(String path, String[] myContribution, boolean allowDirty) throws InterruptedException {
		super(ZkSessionManager.instance());
		this.set = new HashSet<String>();
		this.successfulContributions = new ConcurrentHashMap<String, Boolean>();
		this.failedContributions = new ConcurrentHashMap<String, Boolean>();
		this.rootPath = path;
		this.allowDirty = allowDirty;
		this.myContribution = myContribution;
		resynchronize();
	}
	
	/**
	 * The complete set of entries created by the contributions of connected ZkContributedSet instances
	 * @return					The set of all the entries in the shared contributed set				
	 */
	public Set<String> getKeySet() {
		return set;
	}
	
	/**
	 * A list of entries that we contributed, and which will be removed when our session to ZooKeeper is closed or expires
	 * @return					The entries that this instance contributed so the set
	 */
	public Set<String> getSuccessfulContributions() {
		Set<String> successful = Collections.newSetFromMap(successfulContributions);
		Collections.unmodifiableSet(successful);
		return successful;
	}
	
	/**
	 * A list of entries that we contributed, but which had already been contributed by someone else.
	 * @return					The entries that this instance tried to contribute to the set, but which were contributed by another ZkContributedSet instance
	 */
	public Set<String> getFailedContributions() {
		Set<String> failed = Collections.newSetFromMap(failedContributions);
		Collections.unmodifiableSet(failed);
		return failed;
	}
	
	/**
	 * Change your entry nodes.
	 * @param myContribution			What you now wish your entries to consist of
	 * @throws KeeperException 
	 * @throws InterruptedException 
	 */
	public void adjustMyContribution(String[] myContribution) throws InterruptedException, KeeperException {
		synchronized (this) {
			// Calculate list of obsolete and new entries
			Set<String> myObsoleteEntries = new HashSet<String>(Arrays.asList(this.myContribution));
			Set<String> myNewEntries = new HashSet<String>(Arrays.asList(myContribution));
			myObsoleteEntries.removeAll(myNewEntries);
			// Delete obsolete entries
			for (String entry : myObsoleteEntries) {
				zooKeeper().delete(rootPath + "/" + entry, -1);
				successfulContributions.remove(entry);
			}
			// Create new entries
			for (String entry : myNewEntries) {
				try {
					zooKeeper().create(rootPath + "/" + entry, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					recordCreationResult(entry, Code.OK);
				} catch (KeeperException ex) {
					recordCreationResult(entry, ex.code());
				}
			}
		}
	}
			
	@Override 
	protected boolean shouldResurrectOnSessionExpiry() {
		return allowDirty;
	}
	
	@Override
	protected void resynchronize() {
		successfulContributions.clear();
		failedContributions.clear();
		entryNodeCreator.run();
	}	
	
	@Override
	protected void onNodeChildrenChanged(String path) {
		entriesRequestor.run();
	}
	
	private int myEntryIdx;	
	private Runnable entryNodeCreator = new Runnable() {
		
		@Override
		public void run() {
			if (myEntryIdx < myContribution.length) {
				String entryPath = rootPath + "/" + myContribution[myEntryIdx++];
				zooKeeper().create(entryPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
						entryNodeCreatorResultHandler, this);
			} else {
				entriesRequestor.run();
			}
		}
		
	};
	
	private StringCallback entryNodeCreatorResultHandler = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			// In respect to the entry we tried to add, record whether we made the contribution, or another instance
			recordCreationResult(name, Code.get(rc));
			// Execute next step
			if (passOrTryRepeat(rc, new Code[] { Code.OK, Code.NODEEXISTS}, (Runnable)ctx))
					entryNodeCreator.run();
		}
		
	};	
	
	private Runnable entriesRequestor = new Runnable() {

		@Override
		public void run() {
			zooKeeper().getChildren(rootPath, ZkContributedKeySet.this, entriesRequestorResultHandler, this);
		}
	};

	private ChildrenCallback entriesRequestorResultHandler = new ChildrenCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			if (passOrTryRepeat(rc, new Code[] { Code.OK}, (Runnable)ctx)) {
				HashSet<String>modifiableSet = new HashSet<String>(children);
				set = Collections.unmodifiableSet(modifiableSet);
				onStateUpdated();
			}
		}
		
	};	
	
	private void recordCreationResult(String entryName, Code rc) {
		if (rc == Code.OK)
			successfulContributions.put(entryName, true);
		else if (rc == Code.NODEEXISTS) {
			if (!successfulContributions.contains(entryName))
				failedContributions.put(entryName, true);
		}
	}
}
