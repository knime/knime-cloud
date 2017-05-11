package org.knime.cloud.aws.redshift.clustermanipulation.creator;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the RedshiftClusterLauncherNodeModel.
 *
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class RedshiftClusterLauncherNodeFactory extends NodeFactory<RedshiftClusterLauncherNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RedshiftClusterLauncherNodeModel createNodeModel() {
		return new RedshiftClusterLauncherNodeModel();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNrNodeViews() {
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeView<RedshiftClusterLauncherNodeModel> createNodeView(final int viewIndex,
			final RedshiftClusterLauncherNodeModel nodeModel) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasDialog() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new RedshiftClusterLauncherNodeDialog();
	}

}
