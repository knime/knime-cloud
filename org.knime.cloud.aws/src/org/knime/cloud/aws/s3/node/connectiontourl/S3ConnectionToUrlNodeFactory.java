package org.knime.cloud.aws.s3.node.connectiontourl;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "S3ConnectionToUrl" Node.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3ConnectionToUrlNodeFactory extends NodeFactory<S3ConnectionToUrlNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public S3ConnectionToUrlNodeModel createNodeModel() {
		return new S3ConnectionToUrlNodeModel();
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
	public NodeView<S3ConnectionToUrlNodeModel> createNodeView(final int viewIndex,
			final S3ConnectionToUrlNodeModel nodeModel) {
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
		return new S3ConnectionToUrlNodeDialog();
	}

}
