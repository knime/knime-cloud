/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
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
package org.knime.cloud.core.util;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Settings model representing the Azure Blob Store connection information
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public abstract class ConnectionInformationCloudSettings {

	/** The default Timeout */
	public static final int DEFAULT_TIMEOUT = 30000;

	private final SettingsModelAuthentication m_authModel = createAuthenticationModel();
	private final SettingsModelInteger m_timeoutModel = createTimeoutModel();

	private final String m_prefix;

	/**
	 * Constructor.
	 */
	public ConnectionInformationCloudSettings(final String prefix) {
		m_prefix = prefix;
	}

	
	/**
	 * This function should implement the {@link SettingsModelAuthentication} corresponding the the {@link ConnectionInformationCloudComponents}'s {@link DialogComponentAuthentication}.
	 * 
	 * @return the {@link SettingsModelAuthentication} corresponding to the {@link ConnectionInformationCloudComponents}'s {@link DialogComponentAuthentication}.
	 */
	abstract protected SettingsModelAuthentication createAuthenticationModel();

	private SettingsModelInteger createTimeoutModel() {
		return new SettingsModelInteger("timeout", DEFAULT_TIMEOUT);
	}

	/**
	 * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
	 */
	public void saveSettingsTo(final NodeSettingsWO settings) {
		m_authModel.saveSettingsTo(settings);
		m_timeoutModel.saveSettingsTo(settings);
	}

	/**
	 * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
	 */
	public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_authModel.loadSettingsFrom(settings);
		m_timeoutModel.loadSettingsFrom(settings);
	}

	/**
	 * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
	 */
	public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_authModel.validateSettings(settings);
		m_timeoutModel.validateSettings(settings);
	}

	/**
	 * Get the {@link SettingsModelAuthentication}
	 * 
	 * @return The {@link SettingsModelAuthentication}
	 */
	public SettingsModelAuthentication getAuthenticationModel() {
		return m_authModel;
	}

	/**
	 * Get the selected {@link AuthenticationType}.
	 * 
	 * @return The selected {@link AuthenticationType}.
	 */
	public AuthenticationType getAuthenticationType() {
		return m_authModel.getAuthenticationType();
	}

	/**
	 * Get the {@link SettingsModelNumber} for the timeout.
	 * 
	 * @return The {@link SettingsModelNumber} for the timeout.
	 */
	public SettingsModelNumber getTimeoutModel() {
		return m_timeoutModel;
	}

	/**
	 * Get the string value stored for the user
	 * 
	 * @return The string value stored for the user
	 */
	public String getUserValue() {
		return m_authModel.getUsername();
	}

	/**
	 * Get the string value stored for the password.
	 * 
	 * @return The string value stored for the password.
	 */
	public String getPasswordValue() {
		return m_authModel.getPassword();
	}

	
	/**
	 * Whether the workflowcredentials are used or not
	 * 
	 * @return whether the workflowcredentials are used (<code>true</code>) or not (<code>false</code>)
	 */
	public Boolean useWorkflowCredential() {
		return m_authModel.useCredential();
	}

	
	/**
	 * Get the timeout
	 * 
	 * @return The timeout
	 */
	public Integer getTimeout() {
		return m_timeoutModel.getIntValue();
	}
	
	/**
	 * Get the credential
	 * 
	 * @return The workflow credential
	 */
	public String getWorkflowCredential() {
		return m_authModel.getCredential();
	}

	/**
	 * Validate the values for all the {@link SettingsModel}s
	 * 
	 * @throws InvalidSettingsException When a setting is set inappropriately.
	 */
	public void validateValues() throws InvalidSettingsException {
		if (getAuthenticationType().equals(AuthenticationType.USER_PWD) || getAuthenticationType().equals(AuthenticationType.CREDENTIALS)) {

			if (useWorkflowCredential()) {
				if (StringUtils.isBlank(getWorkflowCredential())) {
					throw new InvalidSettingsException("Please enter a valid workflow credential");
				}
			} else {
				if (StringUtils.isBlank(getUserValue())) {
					throw new InvalidSettingsException("Please enter a valid access key id");
				}

				if (StringUtils.isBlank(getPasswordValue())) {
					throw new InvalidSettingsException("Please enter a valid secret access key");
				}
			}
		}

		if (getTimeout() < 0) {
			throw new InvalidSettingsException("Timeout must be a positive number");
		}
	}

	/**
	 * Create the {@link ConnectionInformation} based on the {@link SettingsModel}s.
	 * @param credentialsProvider The {@link CredentialsProvider} 
	 * @param protocol The cloud connectors {@link Protocol}
	 * @return The ConnectionInformation corresponding to the {@link SettingsModel}s.
	 */
	abstract public ConnectionInformation createConnectionInformation(CredentialsProvider credentialsProvider,
			Protocol protocol);

	/**
	 * Returns the connection prefix for this Cloud Settings model
	 * @return this cloud setting model's connection prefix
	 */
	public String getPrefix() {
		return m_prefix;
	}
}