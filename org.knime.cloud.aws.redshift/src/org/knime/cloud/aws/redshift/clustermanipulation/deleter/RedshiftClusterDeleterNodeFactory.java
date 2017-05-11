package org.knime.cloud.aws.redshift.clustermanipulation.deleter;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the RedshiftClusterDeleterNodeModel.
 *
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class RedshiftClusterDeleterNodeFactory extends NodeFactory<RedshiftClusterDeleterNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public RedshiftClusterDeleterNodeModel createNodeModel() {
        return new RedshiftClusterDeleterNodeModel();
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
    public NodeView<RedshiftClusterDeleterNodeModel> createNodeView(final int viewIndex,
        final RedshiftClusterDeleterNodeModel nodeModel) {
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
        return new RedshiftClusterDeleterNodeDialog();
    }

}
