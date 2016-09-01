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

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

/**
 * Settings model representing the Azure Blob Store connection information
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class SettingsAzureBSConnectionInformation {

	public static final int DEFAULT_TIMEOUT = 30000;

	private final SettingsModelAuthentication m_authModel = createAuthenticationModel();
	private final SettingsModelInteger m_timeoutModel = createTimeoutModel();

	private final String m_prefix;

	/**
	 * Constructor.
	 */
	public SettingsAzureBSConnectionInformation(final String prefix) {
		m_prefix = prefix;
	}

	private SettingsModelAuthentication createAuthenticationModel() {
		return new SettingsModelAuthentication("auth", AuthenticationType.USER_PWD, null, null, null);
	}

	private SettingsModelInteger createTimeoutModel() {
		return new SettingsModelInteger("timeout", DEFAULT_TIMEOUT);
	}

	public void saveSettingsTo(final NodeSettingsWO settings) {
		m_authModel.saveSettingsTo(settings);
		m_timeoutModel.saveSettingsTo(settings);
	}

	public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_authModel.loadSettingsFrom(settings);
		m_timeoutModel.loadSettingsFrom(settings);
	}

	public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_authModel.validateSettings(settings);
		m_timeoutModel.validateSettings(settings);
	}

	public SettingsModelAuthentication getAuthenticationModel() {
		return m_authModel;
	}

	public SettingsModelNumber getTimeoutModel() {
		return m_timeoutModel;
	}

	public String getStorageAccount() {
		return m_authModel.getUsername();
	}

	public String getAccessKey() {
		return m_authModel.getPassword();
	}

	public Boolean useWorkflowCredential() {
		return m_authModel.useCredential();
	}

	public Integer getTimeout() {
		return m_timeoutModel.getIntValue();
	}
	public String getWorkflowCredential() {
		return m_authModel.getCredential();
	}

	/**
	 * @param useWorkflowCredential
	 * @param workflowCredential
	 * @param storageAccount
	 * @param accessKey
	 * @param timeout
	 * @throws InvalidSettingsException
	 */
	public static void validateValues(Boolean useWorkflowCredential, String workflowCredential, String storageAccount,
			String accessKey, Integer timeout) throws InvalidSettingsException {
		if (useWorkflowCredential) {
			if (StringUtils.isBlank(workflowCredential)) {
				throw new InvalidSettingsException("Please enter a valid workflow credential");
			}
		} else {
			if (StringUtils.isBlank(storageAccount)) {
				throw new InvalidSettingsException("Please enter a valid access key id");
			}

			if (StringUtils.isBlank(accessKey)) {
				throw new InvalidSettingsException("Please enter a valid secret access key");
			}
		}

		if (timeout < 0) {
			throw new InvalidSettingsException("Timeout must be a positive number");
		}
	}

	/**
	 * @param credentialsProvider
	 * @param protocol
	 * @return
	 */
	public ConnectionInformation createConnectionInformation(CredentialsProvider credentialsProvider,
			Protocol protocol) {

		// Create connection information object
		final ConnectionInformation connectionInformation = new ConnectionInformation();

		connectionInformation.setProtocol(protocol.getName());
		connectionInformation.setHost(AzureBSConnection.HOST);
		connectionInformation.setPort(protocol.getPort());
		connectionInformation.setTimeout(m_timeoutModel.getIntValue());

		// Put storageAccount as user and accessKey as password
		if (useWorkflowCredential()) {
			// Use credentials
			final ICredentials credentials = credentialsProvider.get(getWorkflowCredential());
			connectionInformation.setUser(credentials.getLogin());
			connectionInformation.setPassword(credentials.getPassword());
		} else {
			connectionInformation.setUser(getStorageAccount());
			connectionInformation.setPassword(getAccessKey());
		}

		return connectionInformation;
	}
}