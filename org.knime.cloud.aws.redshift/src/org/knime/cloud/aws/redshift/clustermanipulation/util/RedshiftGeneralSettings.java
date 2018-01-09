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
package org.knime.cloud.aws.redshift.clustermanipulation.util;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Holding general settings for the Amazon Redshift cluster manipulation nodes.
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class RedshiftGeneralSettings {

    /** The default polling interval */
    public static final int DEFAULT_POLLING_INTERVAL = 30;

    private final SettingsModelAuthentication m_authModel = createAuthenticationModel();

    private final SettingsModelInteger m_pollingModel = createPollingModel();

    private final SettingsModelString m_region = createRegionModel();

    private final String m_prefix;

    private SettingsModelString createRegionModel() {
        return new SettingsModelString("region", "us-east-1");
    }

    /**
     * Constructor
     * @param prefix the connection's prefix
     */
    public RedshiftGeneralSettings(final String prefix) {
        m_prefix = prefix;
    }

    private SettingsModelInteger createPollingModel() {
        return new SettingsModelInteger("timeout", DEFAULT_POLLING_INTERVAL);
    }

    /**
     * Returns the selected region.
     *
     * @return the selected region
     */
    public String getRegion() {
        return m_region.getStringValue();
    }

    /**
     * Validates the values for the aws authentication.
     *
     * @throws InvalidSettingsException If the values for the aws authentication are invalid
     */
    public void validateValues() throws InvalidSettingsException {
        if (getAuthenticationType().equals(AuthenticationType.USER_PWD)
            || getAuthenticationType().equals(AuthenticationType.CREDENTIALS)) {

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

        if (getPollingInterval() < 0) {
            throw new InvalidSettingsException("Timeout must be a positive number");
        }
        if (StringUtils.isBlank(getRegion())) {
            throw new InvalidSettingsException("Please enter a valid region");
        }

        if (!Region.getRegion(Regions.fromName(getRegion())).isServiceSupported(getPrefix())) {
            throw new InvalidSettingsException(
                "The region \"" + getRegion() + "\" is not supported by the service \"" + getPrefix() + "\"");
        }

    }

    /**
     * Returns the authentication model for AWS Redshift.
     *
     * @return The authentication model for AWS Redshift
     */
    protected SettingsModelAuthentication createAuthenticationModel() {
        return new SettingsModelAuthentication("auth", AuthenticationType.KERBEROS, null, null, null);
    }

    /**
     * Save all the {@link SettingsModel}s to {@link NodeSettingsWO}.
     *
     * @param settings the {@link NodeSettingsWO} to save to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_authModel.saveSettingsTo(settings);
        m_pollingModel.saveSettingsTo(settings);
        m_region.saveSettingsTo(settings);
    }

    /**
     * Load all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     *
     * @param settings The {@link NodeSettingsRO}to save to
     * @throws InvalidSettingsException If the settings are invalid
     */
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authModel.loadSettingsFrom(settings);
        m_pollingModel.loadSettingsFrom(settings);
        m_region.loadSettingsFrom(settings);
    }

    /**
     * Validate all the {@link SettingsModel}s from {@link NodeSettingsRO}.
     *
     * @param settings the settings to validate from
     * @throws InvalidSettingsException If the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_authModel.validateSettings(settings);
        m_pollingModel.validateSettings(settings);
        m_region.validateSettings(settings);
    }

    /**
     * Get the {@link SettingsModelString} for the region selector.
     *
     * @return the {@link SettingsModelString} for the region selector
     */
    public SettingsModelString getRegionModel() {
        return m_region;
    }

    /**
     * Get the {@link SettingsModelAuthentication}.
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
    public SettingsModelNumber getPollingModel() {
        return m_pollingModel;
    }

    /**
     * Get the string value stored for the user.
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
     * Whether the workflowcredentials are used or not.
     *
     * @return whether the workflowcredentials are used (<code>true</code>) or not (<code>false</code>)
     */
    public Boolean useWorkflowCredential() {
        return m_authModel.useCredential();
    }

    /**
     * Returns the polling interval in milliseconds.
     *
     * @return The polling interval in milliseconds
     */
    public Integer getPollingInterval() {
        return m_pollingModel.getIntValue() * 1000;
    }

    /**
     * Get the credential.
     *
     * @return The workflow credential
     */
    public String getWorkflowCredential() {
        return m_authModel.getCredential();
    }

    /**
     * Returns the connection prefix for this Cloud Settings model.
     *
     * @return this cloud setting model's connection prefix
     */
    public String getPrefix() {
        return m_prefix;
    }

}
