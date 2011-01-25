package org.scale7.zookeeper.cages;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.common.PathUtils;
import org.scale7.portability.SystemProxy;
import org.slf4j.Logger;

/**
 * Create a path on ZooKeeper. First an attempt is made to create the target path directly. If this fails
 * because its immediate ancestor node does not exist, an attempt is made to create the ancestor. This continues
 * until an ancestor node is successfully created. Thereafter, successive descendants are created until the
 * target path is created. This algorithm improves performance in most cases by minimizing round-trips to
 * check the for the existence of ancestors of the target path when the target or a close ancestor already exists.
 *
 * @author dominicwilliams
 *
 */
public class ZkPath extends ZkSyncPrimitive {
	private static final Logger logger = SystemProxy.getLoggerFromFactory(ZkSyncPrimitive.class);

	private final String targetPath;
	private final String[] pathNodes;
	private int pathNodesIdx;
	private final CreateMode createMode;

	public ZkPath(String path) {
		this(path, CreateMode.PERSISTENT);
	}

	public ZkPath(String path, CreateMode createMode) {
		super(ZkSessionManager.instance());
		targetPath = path;
		this.createMode = createMode;
		PathUtils.validatePath(targetPath);
		pathNodes = targetPath.split("/");
		pathNodesIdx = pathNodes.length;
		tryCreatePath.run();
	}

	public String getPath() {
		return targetPath;
	}

	private Runnable tryCreatePath = new Runnable() {

		@Override
		public void run() {
			String toCreate = "/";
			if (pathNodesIdx > 1) {
				StringBuilder currNodePath = new StringBuilder();
				for (int i=1; i<pathNodesIdx; i++) { // i=1 to skip split()'s empty node
					currNodePath.append("/");
					currNodePath.append(pathNodes[i]);
				}
				toCreate = currNodePath.toString();
			}
			//logger.debug("ZkPath calling zooKeeper().create({})", toCreate);

			zooKeeper().create(toCreate, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
					createMode, createPathHandler, this);
		}
	};

	private StringCallback createPathHandler = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			if (passOrTryRepeat(rc, new Code[] { Code.OK, Code.NODEEXISTS, Code.NONODE}, (Runnable)ctx)) {
				Code code = Code.get(rc);
				if (code == Code.OK || code == Code.NODEEXISTS) {
					if (pathNodesIdx >= pathNodes.length) {
						onStateUpdated();
						return;
					}
					pathNodesIdx++;
				} else {
					assert code == Code.NONODE;
					pathNodesIdx--;
				}
				tryCreatePath.run();
			}
		}

	};
}
