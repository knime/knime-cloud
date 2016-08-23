package org.knime.cloud.azure.abs.node.connector;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "ABSConnection" Node.
 *
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSConnectionNodeFactory extends NodeFactory<AzureBSConnectionNodeModel> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AzureBSConnectionNodeModel createNodeModel() {
		return new AzureBSConnectionNodeModel();
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
	public NodeView<AzureBSConnectionNodeModel> createNodeView(final int viewIndex,
			final AzureBSConnectionNodeModel nodeModel) {
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
		return new AzureBSConnectionNodeDialog();
	}

}
