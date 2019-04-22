package org.knime.cloud.aws.comprehend.keyphrases.node;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendKeyPhrasesNodeFactory
        extends NodeFactory<ComprehendKeyPhrasesNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ComprehendKeyPhrasesNodeModel createNodeModel() {
        return new ComprehendKeyPhrasesNodeModel();
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
    public NodeView<ComprehendKeyPhrasesNodeModel> createNodeView(final int viewIndex,
            final ComprehendKeyPhrasesNodeModel nodeModel) {
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
        return new ComprehendKeyPhrasesNodeDialog();
    }

}

