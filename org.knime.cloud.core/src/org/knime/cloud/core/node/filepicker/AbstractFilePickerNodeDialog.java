package org.knime.cloud.core.node.filepicker;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.cloud.core.util.ExpirationComponents;
import org.knime.cloud.core.util.ExpirationSettings;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

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
public abstract class AbstractFilePickerNodeDialog extends NodeDialogPane {

	private final JLabel m_infoLabel;
	private final RemoteFileChooserPanel m_remoteFileChooser;
	private final ExpirationComponents m_ExpirationComp;


	/**
	 * This method must be implemented to check whether the input corresponds to the correct endpoint
	 * @return Whether the input connection corresponds to the correct endpoint
	 * @throws NotConfigurableException
	 */
	abstract protected void checkConnectionInformation(PortObjectSpec portObjectSpec) throws NotConfigurableException;

	/**
	 * The cfgname for the settings given from the NodeModel
	 * @return the cfg from the node model
	 */
	abstract protected String getCfgName();

	/**
	 * Returns the dialgo's {@link ConnectionInformation}
	 * @return the dialog's {@link ConnectionInformation}
	 */
	abstract protected ConnectionInformation getConnectionInformation();

	/**
	 * New pane for configuring the S3ConnectionToUrl node.
	 */
	protected AbstractFilePickerNodeDialog() {
		m_infoLabel = new JLabel();
		final FlowVariableModel fvm = createFlowVariableModel(getCfgName(),
				FlowVariable.Type.STRING);
		m_remoteFileChooser = new RemoteFileChooserPanel(getPanel(), "Remote File", true, "s3RemoteFile",
				RemoteFileChooser.SELECT_FILE, fvm, getConnectionInformation());

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
		settings.addString(getCfgName(), m_remoteFileChooser.getSelection());
		m_ExpirationComp.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
			throws NotConfigurableException {
		checkConnectionInformation(specs[0]);
		m_infoLabel.setText("Connection: " + getConnectionInformation().toURI());

		m_remoteFileChooser.setConnectionInformation(getConnectionInformation());
		m_remoteFileChooser.setSelection(settings.getString(getCfgName(), ""));
		m_ExpirationComp.loadSettingsFrom(settings, specs);
	}
}
