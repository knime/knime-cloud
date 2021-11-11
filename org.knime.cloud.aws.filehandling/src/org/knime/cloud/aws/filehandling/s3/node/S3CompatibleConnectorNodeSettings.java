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
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Consumer;

import org.knime.cloud.aws.filehandling.s3.fs.api.S3FSConnectionConfig;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.connections.base.auth.AuthSettings;
import org.knime.filehandling.core.connections.base.auth.AuthType;
import org.knime.filehandling.core.connections.base.auth.EmptyAuthProviderSettings;
import org.knime.filehandling.core.connections.base.auth.StandardAuthTypes;
import org.knime.filehandling.core.connections.base.auth.UserPasswordAuthProviderSettings;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;

/**
 * Settings model for custom S3 connector that combines the Amazon Authentication and S3 Connector settings with a custom endpoint URL.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class S3CompatibleConnectorNodeSettings extends S3ConnectorNodeSettings {

    private static final String KEY_ENDPOINT_URL = "endpointURL";

    private static final String KEY_REGION = "region";

    private static final String KEY_PATH_STYLE = "pathStyle";

    private static final String KEY_CONNECTION_TIMEOUTS = "connectionTimeoutInSeconds";

    private static final String DEFAULT_ENDPOINT_URL = "";

    private static final String DEFAULT_REGION = "";

    private static final boolean DEFAULT_PATH_STYLE = true;

    private final SettingsModelString m_endpointURL;

    private final AuthSettings m_authSettings;

    private final SettingsModelString m_region;

    private final SettingsModelBoolean m_pathStyle;

    private final SettingsModelIntegerBounded m_connectionTimeout;

    static final AuthType ACCESS_KEY_AND_SECRET_AUTH = new AuthType("accessAndSecretKey",
        "Access Key ID and Secret Key", "Authenticate using Access Key ID and Secret Key");

    static final AuthType DEFAULT_PROVIDER_CHAIN_AUTH = new AuthType("defaultCredProviderChain",
        "Default Credential Provider Chain", "Authenticate using the default Credential Provider Chain");

    /**
     * Creates new instance
     *
     * @param portsConfig Ports configuration.
     */
    S3CompatibleConnectorNodeSettings(final PortsConfiguration portsConfig) {
        super(portsConfig);
        m_endpointURL = new SettingsModelString(KEY_ENDPOINT_URL, DEFAULT_ENDPOINT_URL);
        m_authSettings = new AuthSettings.Builder() //
            .add(new EmptyAuthProviderSettings(StandardAuthTypes.ANONYMOUS)) //
            .add(new UserPasswordAuthProviderSettings(ACCESS_KEY_AND_SECRET_AUTH, true)) //
            .add(new EmptyAuthProviderSettings(DEFAULT_PROVIDER_CHAIN_AUTH)) //
            .defaultType(ACCESS_KEY_AND_SECRET_AUTH) //
            .build();

        m_region = new SettingsModelString(KEY_REGION, DEFAULT_REGION);
        m_pathStyle = new SettingsModelBoolean(KEY_PATH_STYLE, DEFAULT_PATH_STYLE);
        m_connectionTimeout = new SettingsModelIntegerBounded(KEY_CONNECTION_TIMEOUTS,
            S3FSConnectionConfig.DEFAULT_CONNECTION_TIMEOUT_SECONDS, 0, Integer.MAX_VALUE);
    }

    /**
     * @return settings model of endpoint URL
     */
    public SettingsModelString getEndpointURLModel() {
        return m_endpointURL;
    }

    /**
     * @return custom endpoint URL
     */
    public URI getEndpointURL() {
        return URI.create(m_endpointURL.getStringValue());
    }

    /**
     * @return authentication settings
     */
    public AuthSettings getAuthSettings() {
        return m_authSettings;
    }

    /**
     * @return settings model of region
     */
    public SettingsModelString getRegionModel() {
        return m_region;
    }

    /**
     * @return region, might be empty
     */
    public String getRegion() {
        return m_region.getStringValue();
    }

    /**
     * @return path style settings model
     */
    public SettingsModelBoolean getPathStyleModel() {
        return m_pathStyle;
    }

    /**
     * @return {@code true} if client should always use path style access
     */
    public boolean usePathStyle() {
        return m_pathStyle.getBooleanValue();
    }

    /**
     * @return model of the connection timeout in seconds
     */
    public SettingsModelIntegerBounded getConnectionTimeoutModel() {
        return m_connectionTimeout;
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node
     * model).
     *
     * @param settings
     *            The settings.
     */
    @Override
    public void saveSettingsForModel(final NodeSettingsWO settings) {
        super.saveSettingsForModel(settings);
        m_endpointURL.saveSettingsTo(settings);
        m_authSettings.saveSettingsForModel(settings.addNodeSettings(AuthSettings.KEY_AUTH));
        m_region.saveSettingsTo(settings);
        m_pathStyle.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
    }

    /**
     * Saves settings to the given {@link NodeSettingsWO} (to be called by the node
     * dialog).
     *
     * @param settings
     *            The settings.
     */
    @Override
    public void saveSettingsForDialog(final NodeSettingsWO settings) {
        super.saveSettingsForDialog(settings);
        m_endpointURL.saveSettingsTo(settings);
        // m_authSettings are also saved by AuthenticationDialog
        m_region.saveSettingsTo(settings);
        m_pathStyle.saveSettingsTo(settings);
        m_connectionTimeout.saveSettingsTo(settings);
    }


    /**
     * Validates the settings in a given {@link NodeSettingsRO}
     *
     * @param settings Node settings.
     * @throws InvalidSettingsException
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_endpointURL.validateSettings(settings);
        m_authSettings.validateSettings(settings.getNodeSettings(AuthSettings.KEY_AUTH));
        m_region.validateSettings(settings);
        m_pathStyle.validateSettings(settings);
        m_connectionTimeout.validateSettings(settings);
        super.validateSettings(settings);

        final var tmpSettings = new S3CompatibleConnectorNodeSettings(m_portConfig);
        tmpSettings.loadSettingsForModel(settings);
        tmpSettings.validate();
    }

    /**
     * Validate values of this settings instance.
     *
     * @throws InvalidSettingsException on invalid settings
     */
    @Override
    @SuppressWarnings("unused")
    public void validate() throws InvalidSettingsException {
        super.validate();

        final var endpointUrl = m_endpointURL.getStringValue();
        if (endpointUrl == null || endpointUrl.isBlank()) {
            throw new InvalidSettingsException("URL required on endpoint override.");
        }

        try {
            new URI(endpointUrl);
        } catch (final URISyntaxException ex) {
            throw new InvalidSettingsException("Invalid endpoint URL: " + ex.getMessage(), ex);
        }

        m_authSettings.validate();
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the
     * node model).
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    @Override
    public void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsForModel(settings);
        m_endpointURL.loadSettingsFrom(settings);
        m_authSettings.loadSettingsForModel(settings.getNodeSettings(AuthSettings.KEY_AUTH));
        m_region.loadSettingsFrom(settings);
        m_pathStyle.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO} (to be called by the
     * node dialog).
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsForDialog(settings);
        m_endpointURL.loadSettingsFrom(settings);
        // m_authSettings are loaded by AuthenticationDialog
        m_region.loadSettingsFrom(settings);
        m_pathStyle.loadSettingsFrom(settings);
        m_connectionTimeout.loadSettingsFrom(settings);
    }

    void configureInModel(final PortObjectSpec[] inSpecs, final CredentialsProvider credentialsProvider)
        throws InvalidSettingsException {
        final Consumer<StatusMessage> statusConsumer = m -> {
        };
        m_authSettings.configureInModel(inSpecs, statusConsumer, credentialsProvider);
    }

    private CloudConnectionInformation toCloudConnInfo(final CredentialsProvider credentials) throws InvalidSettingsException {
        final var connInfo = new CloudConnectionInformation();
        connInfo.setProtocol("s3");
        connInfo.setTimeout(m_connectionTimeout.getIntValue() * 1000);  // timeout in milliseconds

        final var region = m_region.getStringValue();
        if (region != null && !region.isEmpty()) {
            connInfo.setHost(region);
        }

        if(m_authSettings.getAuthType() == StandardAuthTypes.ANONYMOUS) {
            connInfo.setUseAnonymous(true);
        } else if(m_authSettings.getAuthType() == ACCESS_KEY_AND_SECRET_AUTH) {
            final UserPasswordAuthProviderSettings userPassSettings = m_authSettings
                    .getSettingsForAuthType(ACCESS_KEY_AND_SECRET_AUTH);
            connInfo.setUser(userPassSettings.getUser(credentials::get));
            connInfo.setPassword(userPassSettings.getPassword(credentials::get));
        } else if(m_authSettings.getAuthType() == DEFAULT_PROVIDER_CHAIN_AUTH) {
            connInfo.setUseKerberos(true);
        } else {
            throw new InvalidSettingsException("Unsupported authentication");
        }

        return connInfo;
    }

    /**
     * Create {@link S3FSConnectionConfig} instance from this settings model.
     *
     * @param credentials
     * @return The FSConnectionConfig for S3
     * @throws IOException
     * @throws InvalidSettingsException
     */
    public S3FSConnectionConfig toFSConnectionConfig(final CredentialsProvider credentials)
        throws IOException, InvalidSettingsException {

        final CloudConnectionInformation connInfo = toCloudConnInfo(credentials);
        final S3FSConnectionConfig config = super.toFSConnectionConfig(connInfo, credentials);
        config.setOverrideEndpoint(true);
        config.setEndpointUrl(getEndpointURL());
        config.setPathStyle(usePathStyle());
        return config;
    }
}
