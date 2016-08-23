/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Jul 31, 2016 (budiyanto): created
 */
package org.knime.cloud.azure.abs.node.fileselector;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentDate;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class AzureBSFilePickerNodeDialog extends NodeDialogPane {

	private final JLabel m_infoLabel;
	private final RemoteFileChooserPanel m_remoteFileChooser;
	private ConnectionInformation m_connectionInformation;
	private final DialogComponentDate m_dateComp;

	/**
	 * New pane for configuring the S3ConnectionToUrl node.
	 */
	protected AzureBSFilePickerNodeDialog() {
		m_infoLabel = new JLabel();
		final FlowVariableModel fvm = createFlowVariableModel(AzureBSFilePickerNodeModel.CFG_FILE_SELECTION,
				FlowVariable.Type.STRING);
		m_remoteFileChooser = new RemoteFileChooserPanel(getPanel(), "Remote File", true, "absRemoteFile",
				RemoteFileChooser.SELECT_FILE, fvm, m_connectionInformation);

		m_dateComp = new DialogComponentDate(AzureBSFilePickerNodeModel.createExpirationSettingsModel(),
				"Expiration Time", false);
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
		panel.add(m_dateComp.getComponentPanel(), gbc);
		return panel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		m_dateComp.saveSettingsTo(settings);
		settings.addString(AzureBSFilePickerNodeModel.CFG_FILE_SELECTION, m_remoteFileChooser.getSelection());
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
					|| !m_connectionInformation.getProtocol().equals(AzureBSConnection.PREFIX)) {
				throw new NotConfigurableException("No ABS connection information is available");
			}
			m_infoLabel.setText("Connection: " + m_connectionInformation.toURI());
		} else {
			throw new NotConfigurableException("No ABS connection information available");
		}

		m_remoteFileChooser.setConnectionInformation(m_connectionInformation);
		m_remoteFileChooser.setSelection(settings.getString(AzureBSFilePickerNodeModel.CFG_FILE_SELECTION, ""));
		m_dateComp.loadSettingsFrom(settings, specs);
	}

}
