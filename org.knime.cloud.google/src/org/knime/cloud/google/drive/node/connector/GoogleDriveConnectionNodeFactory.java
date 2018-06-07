package org.knime.cloud.google.drive.node.connector;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class GoogleDriveConnectionNodeFactory extends NodeFactory<GoogleDriveConnectionNodeModel> {

	@Override
	public GoogleDriveConnectionNodeModel createNodeModel() {
		// TODO Auto-generated method stub
		return new GoogleDriveConnectionNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public NodeView<GoogleDriveConnectionNodeModel> createNodeView(int viewIndex,
			GoogleDriveConnectionNodeModel nodeModel) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean hasDialog() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		// TODO Auto-generated method stub
		return null;
	}


}
