package org.knime.cloud.aws.s3.node.filepicker;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.cloud.aws.ExpirationComponents;
import org.knime.cloud.aws.ExpirationSettings;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

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
public class S3FilePickerNodeDialog extends NodeDialogPane {

	private final JLabel m_infoLabel;
	private final RemoteFileChooserPanel m_remoteFileChooser;
	private ConnectionInformation m_connectionInformation;
	private final ExpirationComponents m_ExpirationComp;



	/**
	 * New pane for configuring the S3ConnectionToUrl node.
	 */
	protected S3FilePickerNodeDialog() {
		m_infoLabel = new JLabel();
		final FlowVariableModel fvm = createFlowVariableModel(S3FilePickerNodeModel.CFG_FILE_SELECTION,
				FlowVariable.Type.STRING);
		m_remoteFileChooser = new RemoteFileChooserPanel(getPanel(), "Remote File", true, "s3RemoteFile",
				RemoteFileChooser.SELECT_FILE, fvm, m_connectionInformation);

		m_ExpirationComp  = new ExpirationComponents(new ExpirationSettings());


		// add Time to expiration
		addTab("Options", initLayout());
	}

	private JPanel initLayout() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		NodeUtils.resetGBC(gbc);
		gbc.weightx = 1;
		panel.add(m_infoLabel, gbc);
		gbc.gridy++;
		panel.add(m_remoteFileChooser.getPanel(), gbc);
		gbc.gridy++;
		panel.add(m_ExpirationComp.getDialogPanel(),gbc);
		return panel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		settings.addString(S3FilePickerNodeModel.CFG_FILE_SELECTION, m_remoteFileChooser.getSelection());
		m_ExpirationComp.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
			throws NotConfigurableException {
		if (specs[0] != null) {
			final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) specs[0];
			m_connectionInformation = object.getConnectionInformation();
			// Check if the port object has connection information
			if (m_connectionInformation == null
					|| !m_connectionInformation.getProtocol().equals(AmazonS3.ENDPOINT_PREFIX)) {
				throw new NotConfigurableException("No S3 connection information is available");
			}
			m_infoLabel.setText("Connection: " + m_connectionInformation.toURI());
		} else {
			throw new NotConfigurableException("No S3 connection information available");
		}

		m_remoteFileChooser.setConnectionInformation(m_connectionInformation);
		m_remoteFileChooser.setSelection(settings.getString(S3FilePickerNodeModel.CFG_FILE_SELECTION, ""));
		m_ExpirationComp.loadSettingsFrom(settings, specs);
	}
}
