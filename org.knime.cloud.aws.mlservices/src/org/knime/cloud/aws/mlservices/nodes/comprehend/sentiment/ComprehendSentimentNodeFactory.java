package org.knime.cloud.aws.mlservices.nodes.comprehend.sentiment;

import org.knime.cloud.aws.mlservices.nodes.comprehend.BaseComprehendNodeDialog;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * The {@code NodeMode} for the Amazon Comprehend Sentiment node.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeFactory extends NodeFactory<ComprehendSentimentNodeModel> {

	@Override
	public ComprehendSentimentNodeModel createNodeModel() {
		return new ComprehendSentimentNodeModel();
	}

	@Override
	public int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<ComprehendSentimentNodeModel> createNodeView(final int viewIndex,
			final ComprehendSentimentNodeModel nodeModel) {
		return null;
	}

	@Override
	public boolean hasDialog() {
		return true;
	}

	@Override
	public NodeDialogPane createNodeDialogPane() {
		return new BaseComprehendNodeDialog(true);
	}

}
