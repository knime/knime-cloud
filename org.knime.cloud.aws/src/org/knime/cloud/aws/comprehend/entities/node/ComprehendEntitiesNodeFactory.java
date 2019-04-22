package org.knime.cloud.aws.comprehend.entities.node;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendEntitiesNodeFactory
        extends NodeFactory<ComprehendEntitiesNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ComprehendEntitiesNodeModel createNodeModel() {
        return new ComprehendEntitiesNodeModel();
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
    public NodeView<ComprehendEntitiesNodeModel> createNodeView(final int viewIndex,
            final ComprehendEntitiesNodeModel nodeModel) {
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
        return new ComprehendEntitiesNodeDialog();
    }

}

