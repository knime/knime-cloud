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
 *   Aug 11, 2016 (oole): created
 */
package org.knime.cloud.azure.abs.util;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class ComponentsAzureBSConnectionInformation {

	protected final SettingsAzureBSConnectionInformation m_settings;

	private DialogComponentAuthentication m_auth;
	private DialogComponentNumber m_timeout;

	/**
	 * Constructor.
	 */
	public ComponentsAzureBSConnectionInformation(final SettingsAzureBSConnectionInformation settings) {
		m_settings = settings;
	}


	protected DialogComponentAuthentication getAuthenticationComponent() {
		m_auth = new DialogComponentAuthentication(m_settings.getAuthenticationModel(), "Authentication", AuthenticationType.USER_PWD, AuthenticationType.CREDENTIALS);
		m_auth.setUsernameLabel("Storage Account: ");
		m_auth.setPasswordLabel("Access Key: ");
		return m_auth;
	}

	protected DialogComponentNumber getTimeoutComponent() {
		m_timeout = new DialogComponentNumber(m_settings.getTimeoutModel(), "Timeout", 100);
		return m_timeout;
	}


	public JPanel getDialogPanel() {
		final JPanel panel = new JPanel(new GridBagLayout());
		final GridBagConstraints gbc = new GridBagConstraints();
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.weightx = 1;
		gbc.gridx = 0;
		gbc.gridy = 0;
		panel.add(getAuthenticationComponent().getComponentPanel(), gbc);
		gbc.gridy++;
		panel.add(getTimeoutComponent().getComponentPanel(),gbc);

		return panel;
	}

	public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs, final CredentialsProvider cp) throws NotConfigurableException {
		m_auth.loadSettingsFrom(settings, specs, cp);
		m_timeout.loadSettingsFrom(settings, specs);
	}

	public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
		m_auth.saveSettingsTo(settings);
		m_timeout.saveSettingsTo(settings);
	}

	public SettingsAzureBSConnectionInformation getModel() {
		return m_settings;
	}
}
