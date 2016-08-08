package org.knime.cloud.aws.s3.node.connector;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "S3Connection" Node.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3ConnectionNodeFactory extends NodeFactory<S3ConnectionNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public S3ConnectionNodeModel createNodeModel() {
		return new S3ConnectionNodeModel();
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
	public NodeView<S3ConnectionNodeModel> createNodeView(final int viewIndex,
			final S3ConnectionNodeModel nodeModel) {
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
		return new S3ConnectionNodeDialog();
	}

}
