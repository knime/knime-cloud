package org.knime.cloud.aws.comprehend.node.sentiment;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeFactory
        extends NodeFactory<ComprehendSentimentNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ComprehendSentimentNodeModel createNodeModel() {
        return new ComprehendSentimentNodeModel();
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
    public NodeView<ComprehendSentimentNodeModel> createNodeView(final int viewIndex,
            final ComprehendSentimentNodeModel nodeModel) {
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
        return new ComprehendSentimentNodeDialog();
    }

}

