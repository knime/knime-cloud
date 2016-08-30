package org.knime.cloud.azure.abs.node.filepicker;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "ABSConnection" Node.
 *
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSFilePickerNodeFactory extends NodeFactory<AzureBSFilePickerNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AzureBSFilePickerNodeModel createNodeModel() {
		return new AzureBSFilePickerNodeModel();
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
	public NodeView<AzureBSFilePickerNodeModel> createNodeView(final int viewIndex,
			final AzureBSFilePickerNodeModel nodeModel) {
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
		return new AzureBSFilePickerNodeDialog();
	}

}
