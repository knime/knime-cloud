package org.knime.cloud.aws.nodes.tagging.comprehend.syntax;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class SyntaxTaggerNodeFactory
        extends NodeFactory<SyntaxTaggerNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SyntaxTaggerNodeModel createNodeModel() {
        return new SyntaxTaggerNodeModel();
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
    public NodeView<SyntaxTaggerNodeModel> createNodeView(final int viewIndex,
            final SyntaxTaggerNodeModel nodeModel) {
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
        return new SyntaxTaggerNodeDialog();
    }

}

