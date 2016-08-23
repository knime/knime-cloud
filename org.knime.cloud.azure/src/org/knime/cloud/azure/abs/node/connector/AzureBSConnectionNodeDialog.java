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
package org.knime.cloud.azure.abs.node.connector;

import java.awt.Container;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JPanel;

import org.knime.base.filehandling.remote.connectioninformation.node.TestConnectionDialog;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.cloud.azure.abs.filehandler.AzureBSRemoteFileHandler;
import org.knime.cloud.azure.abs.util.ComponentsAzureBSConnectionInformation;
import org.knime.cloud.azure.abs.util.SettingsAzureBSConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSConnectionNodeDialog extends NodeDialogPane {

	private final SettingsAzureBSConnectionInformation m_settings = AzureBSConnectionNodeModel.createAzureBSSettings();
	private final ComponentsAzureBSConnectionInformation m_components = new ComponentsAzureBSConnectionInformation(m_settings);

	public AzureBSConnectionNodeDialog() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5,5,5,5);
		gbc.weightx = 1;
		gbc.weighty = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
		panel.add(m_components.getDialogPanel(), gbc);
		gbc.gridy++;
		gbc.anchor = GridBagConstraints.ABOVE_BASELINE_LEADING;
		final JButton testConnectionButton = new JButton("Test connection");
		testConnectionButton.addActionListener(new TestConnectionListener());
		panel.add(testConnectionButton,gbc);
		addTab("Options", panel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		m_components.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
			throws NotConfigurableException {
		final CredentialsProvider cp = getCredentialsProvider();
		m_components.loadSettingsFrom(settings, specs, cp);
	}

	/**
	 * Listener that opens the test connection dialog.
	 *
	 *
	 * @author Patrick Winter, KNIME.com, Zurich, Switzerland
	 */
	private class TestConnectionListener implements ActionListener {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void actionPerformed(final ActionEvent e) {
			// Get frame
			Frame frame = null;
			Container container = getPanel().getParent();
			while (container != null) {
				if (container instanceof Frame) {
					frame = (Frame) container;
					break;
				}
				container = container.getParent();
			}

			// Get connection information to current settings
			final SettingsAzureBSConnectionInformation model = m_components.getModel();
			final ConnectionInformation connectionInformation = model
					.createConnectionInformation(getCredentialsProvider(), AzureBSRemoteFileHandler.PROTOCOL);

			new TestConnectionDialog(connectionInformation).open(frame);
		}

	}
}
