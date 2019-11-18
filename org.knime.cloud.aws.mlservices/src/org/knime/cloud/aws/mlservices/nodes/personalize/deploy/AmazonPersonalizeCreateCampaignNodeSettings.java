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
 *   Oct 30, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.deploy;

import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Node settings for Amazon Personalize campaign creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateCampaignNodeSettings {

    private static final String CFG_KEY_CAMPAIGN_NAME = "campaign_name";

    private static final String CFG_KEY_SOLUTION_VERSION = "solution_version";

    private static final String CFG_KEY_MIN_PROVISIONED_TPS = "min_provisioned_tps";

    private static final String CFG_KEY_OUTPUT_ARN_AS_VARIABLE = "output_campaign_arn_as_variable";

    private static final String DEF_CAMPAIGN_NAME = "new-campaign";

    private static final NameArnPair DEF_SOLUTION_VERSION = null;

    private static final int DEF_MIN_NUM_PROVISIONed_TPS = 1;

    private static final boolean DEF_OUTPUT_ARN_AS_VARIABLE = false;

    private String m_campaignName = DEF_CAMPAIGN_NAME;

    private NameArnPair m_solutionVersion = DEF_SOLUTION_VERSION;

    private int m_minProvisionedTPS = DEF_MIN_NUM_PROVISIONed_TPS;

    private boolean m_outputCampaignArnAsVar = DEF_OUTPUT_ARN_AS_VARIABLE;

    /**
     * @return the campaignName
     */
    public String getCampaignName() {
        return m_campaignName;
    }

    /**
     * @param campaignName the campaignName to set
     * @throws InvalidSettingsException if the campaignName is empty
     */
    public void setCampaignName(final String campaignName) throws InvalidSettingsException {
        if (campaignName.trim().isEmpty()) {
            throw new InvalidSettingsException("The campaign name must not be empty.");
        }
        m_campaignName = campaignName;
    }

    /**
     * @return the solutionVersion
     */
    public NameArnPair getSolutionVersion() {
        return m_solutionVersion;
    }

    /**
     * @param solutionVersion the solutionVersion to set
     */
    public void setSolutionVersion(final NameArnPair solutionVersion) {
        m_solutionVersion = solutionVersion;
    }

    /**
     * @return the minProvisionedTPS
     */
    public int getMinProvisionedTPS() {
        return m_minProvisionedTPS;
    }

    /**
     * @param minProvisionedTPS the minProvisionedTPS to set
     * @throws InvalidSettingsException if minProvisionedTPS < 1 or minProvisionedTPS > 500
     */
    public void setMinProvisionedTPS(final int minProvisionedTPS) throws InvalidSettingsException {
        if (minProvisionedTPS < 1 || minProvisionedTPS > 500) {
            throw new InvalidSettingsException("The minimum provisioned transactions per second must be in [1,500].");
        }
        m_minProvisionedTPS = minProvisionedTPS;
    }

    /**
     * @return the outputCampaignArnAsVar
     */
    public boolean isOutputCampaignArnAsVar() {
        return m_outputCampaignArnAsVar;
    }

    /**
     * @param outputCampaignArnAsVar the outputCampaignArnAsVar to set
     */
    public void setOutputCampaignArnAsVar(final boolean outputCampaignArnAsVar) {
        m_outputCampaignArnAsVar = outputCampaignArnAsVar;
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if some settings are missing
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_solutionVersion = NameArnPair.loadSettings(settings, CFG_KEY_SOLUTION_VERSION);
        m_campaignName = settings.getString(CFG_KEY_CAMPAIGN_NAME);
        m_minProvisionedTPS = settings.getInt(CFG_KEY_MIN_PROVISIONED_TPS);
        m_outputCampaignArnAsVar = settings.getBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE);
    }

    /**
     * Loads the settings from the node settings object using default values if some settings are missing.
     *
     * @param settings a node settings object
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_solutionVersion = NameArnPair.loadSettingsForDialog(settings, CFG_KEY_SOLUTION_VERSION, DEF_SOLUTION_VERSION);
        m_campaignName = settings.getString(CFG_KEY_CAMPAIGN_NAME, DEF_CAMPAIGN_NAME);
        m_minProvisionedTPS = settings.getInt(CFG_KEY_MIN_PROVISIONED_TPS, DEF_MIN_NUM_PROVISIONed_TPS);
        m_outputCampaignArnAsVar = settings.getBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE, DEF_OUTPUT_ARN_AS_VARIABLE);
    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {
        if (m_solutionVersion != null) {
            m_solutionVersion.saveSettings(settings, CFG_KEY_SOLUTION_VERSION);
        }
        settings.addString(CFG_KEY_CAMPAIGN_NAME, m_campaignName);
        settings.addInt(CFG_KEY_MIN_PROVISIONED_TPS, m_minProvisionedTPS);
        settings.addBoolean(CFG_KEY_OUTPUT_ARN_AS_VARIABLE, m_outputCampaignArnAsVar);
    }
}
