package org.knime.cloud.aws.s3.node.filepicker;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.core.node.filepicker.AbstractFilePickerNodeDialog;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;

import com.amazonaws.services.s3.AmazonS3;

/**
 * <code>NodeDialog</code> for the "S3ConnectionToUrl" Node.
 *
 *
 * This node dialog derives from {@link DefaultNodeSettingsPane} which allows
 * creation of a simple dialog with standard components. If you need a more
 * complex dialog please derive directly from
 * {@link org.knime.core.node.NodeDialogPane}.
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3FilePickerNodeDialog extends AbstractFilePickerNodeDialog {

	private ConnectionInformation m_connectionInformation;

	/**
	 *
	 * {@inheritDoc}
	 */
	@Override
	protected void checkConnectionInformation(PortObjectSpec spec) throws NotConfigurableException {
		if (spec != null) {
			final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) spec;
			final ConnectionInformation connectionInformation = object.getConnectionInformation();
			// Check if the port object has connection information
			if (connectionInformation == null
					|| !connectionInformation.getProtocol().equals(AmazonS3.ENDPOINT_PREFIX)) {
				throw new NotConfigurableException("No S3 connection information is available");
			}
			m_connectionInformation = connectionInformation;
		} else {
			throw new NotConfigurableException("No S3 connection information available");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getCfgName() {
		return S3FilePickerNodeModel.CFG_FILE_SELECTION;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected ConnectionInformation getConnectionInformation() {
		return m_connectionInformation;
	}

}
