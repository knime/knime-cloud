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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
package org.knime.cloud.aws.util;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.cloud.core.util.ConnectionInformationCloudSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Model representing AWS connection information
 *
 * @author Budi Yanto, KNIME.com
 */
public class AWSConnectionInformationSettings extends ConnectionInformationCloudSettings {

	private final SettingsModelString m_region = createRegionModel();
	private final SettingsModelBoolean m_sSEncryption = createSSEncrpyionModel();

	private final static String SSE_KEY = "ssencryption";

	private SettingsModelString createRegionModel() {
		return new SettingsModelString("region", "us-east-1");
	}

	private SettingsModelBoolean createSSEncrpyionModel() {
	    return new SettingsModelBoolean(SSE_KEY, false);
	}


	/**
	 * @param prefix
	 */
	public AWSConnectionInformationSettings(final String prefix) {
		super(prefix);
	}

	/**
	 * Returns the selected region
	 * @return the selected region
	 */
	public String getRegion() {
		return m_region.getStringValue();
	}

	@Override
	public void validateValues() throws InvalidSettingsException {
		super.validateValues();
		if (StringUtils.isBlank(getRegion())) {
			throw new InvalidSettingsException("Please enter a valid region");
		}

		if (!Region.getRegion(Regions.fromName(getRegion())).isServiceSupported(getPrefix())) {
			throw new InvalidSettingsException(
				"The region \"" + getRegion() + "\" is not supported by the service \"" + getPrefix() + "\"");
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected SettingsModelAuthentication createAuthenticationModel() {
		return new SettingsModelAuthentication("auth", AuthenticationType.KERBEROS, null, null, null);
	}

	/**
	 * Returns the {@link ConnectionInformation} resulting from the settings
	 *
	 * @param credentialsProvider the credentials provider
	 * @param protocol the protocol
	 * @return this settings ConnectionInformation
	 */
	@Override
	public CloudConnectionInformation createConnectionInformation(final CredentialsProvider credentialsProvider,
		final Protocol protocol) {

		// Create connection information object
		final CloudConnectionInformation connectionInformation = new CloudConnectionInformation();

		connectionInformation.setProtocol(protocol.getName());
		connectionInformation.setHost(getRegion());
		connectionInformation.setPort(protocol.getPort());
		connectionInformation.setTimeout(getTimeout());

		// Set the field "useKerberos" to true if "Default Credentials Provider Chain" should be used, otherwise to false
		connectionInformation.setUseKeyChain(getAuthenticationModel().getAuthenticationType().equals(AuthenticationType.KERBEROS));

		if(getAuthenticationModel().getAuthenticationType().equals(AuthenticationType.KERBEROS)) {
		    connectionInformation.setUser("*****");
		    connectionInformation.setPassword(null);
		} else {
		    // Put accessKeyId as user and secretAccessKey as password
	        if (useWorkflowCredential()) {
	            // Use credentials
	            final ICredentials credentials = credentialsProvider.get(getWorkflowCredential());
	            connectionInformation.setUser(credentials.getLogin());
	            connectionInformation.setPassword(credentials.getPassword());
	        } else {
	            connectionInformation.setUser(getUserValue());
	            connectionInformation.setPassword(getPasswordValue());
	        }
		}

		connectionInformation.setUseSSEncryption(getSSEncryptionModel().getBooleanValue());

		return connectionInformation;
	}

	@Override
	public void saveSettingsTo(final NodeSettingsWO settings) {
		super.saveSettingsTo(settings);
		m_region.saveSettingsTo(settings);
		// New Server Side Encryption AP-8823
		m_sSEncryption.saveSettingsTo(settings);
	}

	@Override
	public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		super.loadValidatedSettings(settings);
		m_region.loadSettingsFrom(settings);
		// New Server Side Encryption AP-8823
		if (settings.containsKey(SSE_KEY)) {
		    m_sSEncryption.loadSettingsFrom(settings);
		}
	}

	@Override
	public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		super.validateSettings(settings);
		m_region.validateSettings(settings);
		// New Server Side Encryption AP-8823
		if (settings.containsKey(SSE_KEY)) {
		    m_sSEncryption.validateSettings(settings);
		}
	}

	/**
	 * Get the {@link SettingsModelString} for the region selector
	 * @return the {@link SettingsModelString} for the region selector
	 */
	public SettingsModelString getRegionModel() {
		return m_region;
	}

	/**
	 * Get the {@link SettingsModelBoolean} for the Server Side Encryption.
	 * @return the {@link SettingsModelBoolean} for the Server Side Encryption
	 */
	public SettingsModelBoolean getSSEncryptionModel() {
	    return m_sSEncryption;
	}


}
