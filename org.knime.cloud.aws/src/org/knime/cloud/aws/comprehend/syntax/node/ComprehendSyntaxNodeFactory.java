package org.knime.cloud.aws.comprehend.syntax.node;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSyntaxNodeFactory
        extends NodeFactory<ComprehendSyntaxNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ComprehendSyntaxNodeModel createNodeModel() {
        return new ComprehendSyntaxNodeModel();
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
    public NodeView<ComprehendSyntaxNodeModel> createNodeView(final int viewIndex,
            final ComprehendSyntaxNodeModel nodeModel) {
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
        return new ComprehendSyntaxNodeDialog();
    }

}

