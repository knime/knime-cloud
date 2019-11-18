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

import java.io.File;
import java.io.IOException;

import org.knime.cloud.aws.mlservices.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils.Status;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.CreateCampaignRequest;
import com.amazonaws.services.personalize.model.CreateCampaignResult;
import com.amazonaws.services.personalize.model.DeleteCampaignRequest;
import com.amazonaws.services.personalize.model.DescribeCampaignRequest;
import com.amazonaws.services.personalize.model.DescribeCampaignResult;

/**
 * Node model for Amazon Personalize campaign creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateCampaignNodeModel extends NodeModel {

    private AmazonPersonalizeCreateCampaignNodeSettings m_settings;

    /**
     */
    protected AmazonPersonalizeCreateCampaignNodeModel() {
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE}, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (m_settings == null) {
            throw new InvalidSettingsException("The node must be configured.");
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final CloudConnectionInformation cxnInfo =
            ((AmazonConnectionInformationPortObject)inObjects[0]).getConnectionInformation();

        try (final AmazonPersonalizeConnection personalizeConnection = new AmazonPersonalizeConnection(cxnInfo)) {
            final AmazonPersonalize personalizeClient = personalizeConnection.getClient();
            final CreateCampaignRequest createCampaignRequest = new CreateCampaignRequest();
            final CreateCampaignResult campaign = personalizeClient.createCampaign(createCampaignRequest
                .withName(m_settings.getCampaignName()).withSolutionVersionArn(m_settings.getSolutionVersion().getARN())
                .withMinProvisionedTPS(m_settings.getMinProvisionedTPS()));

            // TODO Test update of existing campaign
            try {
                final DescribeCampaignRequest describeCampaignRequest =
                    new DescribeCampaignRequest().withCampaignArn(campaign.getCampaignArn());
                AmazonPersonalizeUtils.waitUntilActive(() -> {
                    final DescribeCampaignResult campaignDescription =
                        personalizeClient.describeCampaign(describeCampaignRequest);
                    final String status = campaignDescription.getCampaign().getStatus();
                    exec.setMessage("Creating campaign (Status: " + status + ")");
                    if (status.equals(Status.CREATED_FAILED.getStatus())) {
                        personalizeClient.deleteCampaign(new DeleteCampaignRequest()
                            .withCampaignArn(campaignDescription.getCampaign().getCampaignArn()));
                        throw new IllegalStateException("No campaign has been created. Reason: "
                            + campaignDescription.getCampaign().getFailureReason());
                    }
                    return status.equals(Status.ACTIVE.getStatus());
                }, 1000);
            } catch (InterruptedException e) {
                // Cancel job
                // TODO
                throw e;
            }

            if (m_settings.isOutputCampaignArnAsVar()) {
                pushFlowVariableString("campaign-ARN", campaign.getCampaignArn());
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_settings != null) {
            m_settings.saveSettings(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (m_settings == null) {
            m_settings = new AmazonPersonalizeCreateCampaignNodeSettings();
        }
        m_settings.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to do
    }

}
