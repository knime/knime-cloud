package org.knime.cloud.aws.nodes.tagging.comprehend.entities;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class EntityTaggerNodeFactory
        extends NodeFactory<EntiityTaggerNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public EntiityTaggerNodeModel createNodeModel() {
        return new EntiityTaggerNodeModel();
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
    public NodeView<EntiityTaggerNodeModel> createNodeView(final int viewIndex,
            final EntiityTaggerNodeModel nodeModel) {
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
        return new EntityTaggerNodeDialog();
    }

}

