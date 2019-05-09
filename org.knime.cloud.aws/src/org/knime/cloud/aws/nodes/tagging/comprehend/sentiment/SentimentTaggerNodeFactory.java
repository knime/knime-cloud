package org.knime.cloud.aws.nodes.tagging.comprehend.sentiment;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class SentimentTaggerNodeFactory
        extends NodeFactory<SentimentTaggerNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SentimentTaggerNodeModel createNodeModel() {
        return new SentimentTaggerNodeModel();
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
    public NodeView<SentimentTaggerNodeModel> createNodeView(final int viewIndex,
            final SentimentTaggerNodeModel nodeModel) {
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

