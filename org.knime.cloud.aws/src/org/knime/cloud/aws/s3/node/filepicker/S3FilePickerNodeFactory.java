package org.knime.cloud.aws.s3.node.filepicker;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "S3ConnectionToUrl" Node.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3FilePickerNodeFactory extends NodeFactory<S3FilePickerNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public S3FilePickerNodeModel createNodeModel() {
		return new S3FilePickerNodeModel();
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
	public NodeView<S3FilePickerNodeModel> createNodeView(final int viewIndex,
			final S3FilePickerNodeModel nodeModel) {
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
		return new S3FilePickerNodeDialog();
	}

}
