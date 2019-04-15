package org.knime.cloud.aws.comprehend.node.language;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "MyExampleNode" Node.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendLanguageNodeFactory
        extends NodeFactory<ComprehendLanguageNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public ComprehendLanguageNodeModel createNodeModel() {
        return new ComprehendLanguageNodeModel();
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
    public NodeView<ComprehendLanguageNodeModel> createNodeView(final int viewIndex,
            final ComprehendLanguageNodeModel nodeModel) {
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
        return new ComprehendLanguageNodeDialog();
    }

}

