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
package org.knime.cloud.aws;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.config.Config;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Model representing AWS connection information
 *
 * @author Budi Yanto, KNIME.com
 */
public class SettingsModelAWSConnectionInformation extends SettingsModel {

	/** Default timeout */
	public static final int DEF_TIMEOUT = 30000;

	private static final String ENCRYPTION_KEY = "5QCYUcXN8jVvAM15";

	private static final String CFG_AUTH_TYPE = "authentication-type";

	private static final String CFG_USE_WORKFLOW_CREDENTIAL = "use-workflow-credential";

	private static final String CFG_WORKFLOW_CREDENTIAL = "workflow-credential";

	private static final String CFG_ACCESS_KEY_ID = "access-key-id";

	private static final String CFG_SECRET_ACCESS_KEY = "secret-access-key";

	private static final String CFG_REGION = "region";

	private static final String CFG_TIMEOUT = "timeout";

	private final String m_configName;

	private final String m_endpointPrefix;

	private AuthenticationType m_authType;

	private boolean m_useWorkflowCredential;

	private String m_workflowCredential;

	private String m_accessKeyId;

	private String m_secretAccessKey;

	private String m_region;

	private int m_timeout;

	/**
	 *
	 * Authentication type for Amazon S3
	 * @see <a href="https://docs.aws.amazon.com/java-sdk/latest/developer-guide/credentials.html">
	 * Working with AWS Credentials</a>
	 */
	public enum AuthenticationType implements ButtonGroupEnumInterface {
		/** Explicitly input access key ID and secret access key */
	    ACCESS_KEY_SECRET("Access Key ID & Secret Key", "Access key ID and secret access key"),

	    /** Use default credential provider chain */
	    DEF_CRED_PROVIDER_CHAIN("Default Credential Provider Chain", "Default credential provider chain");

		private String m_toolTip;

		private String m_text;

		private AuthenticationType(final String text, final String toolTip) {
			m_text = text;
			m_toolTip = toolTip;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getText() {
			return m_text;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getActionCommand() {
			return name();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String getToolTip() {
			return m_toolTip;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isDefault() {
			return this.equals(ACCESS_KEY_SECRET);
		}

		/**
		 * @param actionCommand the action command
		 * @return the {@link AuthenticationType} for the action command
		 */
		public static AuthenticationType get(final String actionCommand) {
			return valueOf(actionCommand);
		}

	}

	/**
	 *
	 * @param configName
	 * @param endpointPrefix
	 */
	public SettingsModelAWSConnectionInformation(final String configName, final String endpointPrefix) {
		this(configName, endpointPrefix, AuthenticationType.DEF_CRED_PROVIDER_CHAIN, false, null, null, null, null,
			DEF_TIMEOUT);
	}

	/**
	 *
	 * @param configName
	 * @param endpointPrefix
	 * @param authType
	 * @param useWorkflowCredential
	 * @param workflowCredential
	 * @param accessKeyId
	 * @param secretAccessKey
	 * @param region
	 * @param timeout
	 */
	public SettingsModelAWSConnectionInformation(final String configName, final String endpointPrefix,
		final AuthenticationType authType, final boolean useWorkflowCredential, final String workflowCredential,
		final String accessKeyId, final String secretAccessKey, final String region, final int timeout) {
		CheckUtils.checkArgument(StringUtils.isNotBlank(configName), "The config name must be a non-empty string");
		CheckUtils.checkArgument(StringUtils.isNotBlank(endpointPrefix),
			"The endpoint prefix must be a non-empty string");
		m_configName = configName;
		m_endpointPrefix = endpointPrefix;
		m_authType = authType;
		m_useWorkflowCredential = useWorkflowCredential;
		m_workflowCredential = workflowCredential;
		m_accessKeyId = accessKeyId;
		m_secretAccessKey = secretAccessKey;
		m_region = region;
		m_timeout = timeout;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected SettingsModelAWSConnectionInformation createClone() {
		return new SettingsModelAWSConnectionInformation(m_configName, m_endpointPrefix, m_authType,
			m_useWorkflowCredential, m_workflowCredential, m_accessKeyId, m_secretAccessKey, m_region, m_timeout);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getModelTypeID() {
		return "SMID_aws";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getConfigName() {
		return m_configName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
		throws NotConfigurableException {
		// use the current value, if no value is stored in the settings
		final Config config;
		try {
			config = settings.getConfig(m_configName);
			setValues(AuthenticationType.get(config.getString(CFG_AUTH_TYPE, m_authType.name())),
				config.getBoolean(CFG_USE_WORKFLOW_CREDENTIAL, m_useWorkflowCredential),
				config.getString(CFG_WORKFLOW_CREDENTIAL, m_workflowCredential),
				config.getString(CFG_ACCESS_KEY_ID, m_accessKeyId),
				config.getPassword(CFG_SECRET_ACCESS_KEY, ENCRYPTION_KEY, m_secretAccessKey),
				config.getString(CFG_REGION, m_region), config.getInt(CFG_TIMEOUT, m_timeout));
		} catch (final InvalidSettingsException ex) {
			throw new NotConfigurableException(ex.getMessage());
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsForDialog(final NodeSettingsWO settings) throws InvalidSettingsException {
		saveSettingsForModel(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
		final Config config = settings.getConfig(m_configName);
		final String type = config.getString(CFG_AUTH_TYPE);
		final AuthenticationType authType = AuthenticationType.get(type);
		final boolean useWorkflowCredential = config.getBoolean(CFG_USE_WORKFLOW_CREDENTIAL);
		final String workflowCredential = config.getString(CFG_WORKFLOW_CREDENTIAL);
		final String accessKeyId = config.getString(CFG_ACCESS_KEY_ID);
		final String secretAccessKey = config.getPassword(CFG_SECRET_ACCESS_KEY, ENCRYPTION_KEY);
		final String region = config.getString(CFG_REGION);
		final int timeout = config.getInt(CFG_TIMEOUT);

		validateValues(authType, useWorkflowCredential, workflowCredential, accessKeyId, secretAccessKey, region,
			m_endpointPrefix, timeout);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
		// no default value, throw an exception instead
		final Config config = settings.getConfig(m_configName);
		setValues(AuthenticationType.get(config.getString(CFG_AUTH_TYPE)),
			config.getBoolean(CFG_USE_WORKFLOW_CREDENTIAL), config.getString(CFG_WORKFLOW_CREDENTIAL),
			config.getString(CFG_ACCESS_KEY_ID), config.getPassword(CFG_SECRET_ACCESS_KEY, ENCRYPTION_KEY),
			config.getString(CFG_REGION), config.getInt(CFG_TIMEOUT));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsForModel(final NodeSettingsWO settings) {
		final Config config = settings.addConfig(m_configName);
		config.addString(CFG_AUTH_TYPE, m_authType.name());
		config.addBoolean(CFG_USE_WORKFLOW_CREDENTIAL, m_useWorkflowCredential);
		config.addString(CFG_WORKFLOW_CREDENTIAL, m_workflowCredential);
		config.addString(CFG_ACCESS_KEY_ID, m_accessKeyId);
		config.addPassword(CFG_SECRET_ACCESS_KEY, ENCRYPTION_KEY, m_secretAccessKey);
		config.addString(CFG_REGION, m_region);
		config.addInt(CFG_TIMEOUT, m_timeout);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName() + " ('" + m_configName + "')";
	}

	public void setValues(final AuthenticationType authType, final boolean useWorkflowCredential,
		final String workflowCredential, final String accessKeyId, final String secretAccessKey, final String region,
		final int timeout) {
		boolean changed = false;
		changed = setAuthenticationType(authType) || changed;
		changed = setUseWorkflowCredential(useWorkflowCredential) || changed;
		changed = setWorkflowCredential(workflowCredential) || changed;
		changed = setAccessKeyId(accessKeyId) || changed;
		changed = setSecretAccessKey(secretAccessKey) || changed;
		changed = setRegion(region) || changed;
		changed = setTimeout(timeout) || changed;
		if (changed) {
			notifyChangeListeners();
		}
	}

	private boolean setAuthenticationType(final AuthenticationType authType) {
		final boolean changed = !authType.name().equals(m_authType.name());
		m_authType = authType;
		return changed;
	}

	private boolean setUseWorkflowCredential(final boolean useWorkflowCredential) {
		final boolean changed = (m_useWorkflowCredential != useWorkflowCredential);
		m_useWorkflowCredential = useWorkflowCredential;
		return changed;
	}

	private boolean setWorkflowCredential(final String workflowCredential) {
		final boolean changed;
		if (workflowCredential == null) {
			changed = (m_workflowCredential != null);
		} else {
			changed = (!workflowCredential.equals(m_workflowCredential));
		}
		m_workflowCredential = workflowCredential;
		return changed;
	}

	private boolean setAccessKeyId(final String accessKeyId) {
		final boolean changed;
		if (accessKeyId == null) {
			changed = (m_accessKeyId != null);
		} else {
			changed = (!accessKeyId.equals(m_accessKeyId));
		}
		m_accessKeyId = accessKeyId;
		return changed;
	}

	private boolean setSecretAccessKey(final String secretAccessKey) {
		final boolean changed;
		if (secretAccessKey == null) {
			changed = (m_secretAccessKey != null);
		} else {
			changed = (!secretAccessKey.equals(m_secretAccessKey));
		}
		m_secretAccessKey = secretAccessKey;
		return changed;
	}

	private boolean setRegion(final String region) {
		final boolean changed;
		if (region == null) {
			changed = (m_region != null);
		} else {
			changed = (!region.equals(m_region));
		}
		m_region = region;
		return changed;
	}

	private boolean setTimeout(final int timeout) {
		final boolean changed = (m_timeout != timeout);
		m_timeout = timeout;
		return changed;
	}

	/**
	 * @return the authentication type
	 */
	public AuthenticationType getAuthenticationType() {
		return m_authType;
	}

	/**
	 * @return the endpoint prefix
	 */
	public String getEndpointPrefix() {
		return m_endpointPrefix;
	}

	/**
	 * @return the useWorkflowCredential
	 */
	public boolean useWorkflowCredential() {
		return m_useWorkflowCredential;
	}

	/**
	 * @return the workflow credential
	 */
	public String getWorkflowCredential() {
		return m_workflowCredential;
	}

	/**
	 * @return the accessKeyId
	 */
	public String getAccessKeyId() {
		return m_accessKeyId;
	}

	/**
	 * @return the secretAccessKey
	 */
	public String getSecretAccessKey() {
		return m_secretAccessKey;
	}

	/**
	 * @return the region
	 */
	public String getRegion() {
		return m_region;
	}

	/**
	 * @return the timeout
	 */
	public int getTimeout() {
		return m_timeout;
	}

	/**
	 *
	 * @param authType
	 * @param useWorkflowCredential
	 * @param workflowCredential
	 * @param accessKeyId
	 * @param secretAccessKey
	 * @param region
	 * @param endpointPrefix
	 * @param timeout
	 * @throws InvalidSettingsException
	 */
	public static void validateValues(final AuthenticationType authType, final boolean useWorkflowCredential,
		    final String workflowCredential, final String accessKeyId, final String secretAccessKey, final String region,
		    final String endpointPrefix, final int timeout) throws InvalidSettingsException {
		switch (authType) {
			case ACCESS_KEY_SECRET:
				if (useWorkflowCredential) {
					if (StringUtils.isBlank(workflowCredential)) {
						throw new InvalidSettingsException("Please enter a valid workflow credential");
					}
				} else {
					if (StringUtils.isBlank(accessKeyId)) {
						throw new InvalidSettingsException("Please enter a valid access key id");
					}

					if (StringUtils.isBlank(secretAccessKey)) {
						throw new InvalidSettingsException("Please enter a valid secret access key");
					}
				}
				break;

			case DEF_CRED_PROVIDER_CHAIN:

				break;
			default:
				break;
		}

		if (StringUtils.isBlank(region)) {
			throw new InvalidSettingsException("Please enter a valid region");
		}

		if (!Region.getRegion(Regions.fromName(region)).isServiceSupported(endpointPrefix)) {
			throw new InvalidSettingsException(
				"The region \"" + region + "\" is not supported by the service \"" + endpointPrefix + "\"");
		}

		if (timeout < 0) {
			throw new InvalidSettingsException("Timeout must be a positive number");
		}
	}

	/**
	 * Returns the {@link ConnectionInformation} resulting from the settings
	 *
	 * @param credentialsProvider the credentials provider
	 * @param protocol the protocol
	 * @return this settings ConnectionInformation
	 */
	public CloudConnectionInformation createConnectionInformation(final CredentialsProvider credentialsProvider,
		final Protocol protocol) {

		// Create connection information object
		final CloudConnectionInformation connectionInformation = new CloudConnectionInformation();

		connectionInformation.setProtocol(protocol.getName());
		connectionInformation.setHost(getRegion());
		connectionInformation.setPort(protocol.getPort());
		connectionInformation.setTimeout(m_timeout);

		// Set the field "useKerberos" to true if "Default Credentials Provider Chain" should be used, otherwise to false
		connectionInformation.setUseKeyChain(m_authType.equals(AuthenticationType.DEF_CRED_PROVIDER_CHAIN));

		if(m_authType.equals(AuthenticationType.DEF_CRED_PROVIDER_CHAIN)) {
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
	            connectionInformation.setUser(getAccessKeyId());
	            connectionInformation.setPassword(getSecretAccessKey());
	        }
		}

		return connectionInformation;
	}

}
