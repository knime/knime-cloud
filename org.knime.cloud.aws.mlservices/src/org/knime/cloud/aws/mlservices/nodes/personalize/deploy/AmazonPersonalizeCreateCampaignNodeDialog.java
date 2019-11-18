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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Arrays;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.mlservices.nodes.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.amazonaws.services.personalize.AmazonPersonalize;

/**
 * Node dialog for Amazon Personalize campaign creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateCampaignNodeDialog extends NodeDialogPane {

    private final JTextField m_textFieldCampaignName = new JTextField();

    private final JComboBox<NameArnPair> m_comboBoxSolutionVersionList = new JComboBox<>();

    private final JSpinner m_spinnerMinNumProvisionedTransactions = new JSpinner(new SpinnerNumberModel(1, 1, 500, 1));

    private final JCheckBox m_checkBoxOutputAsVariable = new JCheckBox("Provide the campaign ARN as flow variable");

    private String[] m_campaignNames;

    AmazonPersonalizeCreateCampaignNodeDialog() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        panel.add(new JLabel("Campaign name"), gbc);
        gbc.gridx++;
        panel.add(m_textFieldCampaignName, gbc);
        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(new JLabel("Solution version"), gbc);
        gbc.gridx++;
        panel.add(m_comboBoxSolutionVersionList, gbc);
        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(new JLabel("Minimum provisioned transactions per second"), gbc);
        gbc.gridx++;
        panel.add(m_spinnerMinNumProvisionedTransactions, gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 3;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.WEST;
        panel.add(m_checkBoxOutputAsVariable, gbc);

        addTab("Options", panel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        if (m_campaignNames != null && Arrays.asList(m_campaignNames).contains(m_textFieldCampaignName.getText())) {
            throw new InvalidSettingsException("A campaign with the specified name already exists.");
        }

        final AmazonPersonalizeCreateCampaignNodeSettings nodeSettings =
            new AmazonPersonalizeCreateCampaignNodeSettings();
        nodeSettings.setCampaignName(m_textFieldCampaignName.getText());
        nodeSettings.setSolutionVersion((NameArnPair)m_comboBoxSolutionVersionList.getSelectedItem());
        nodeSettings.setMinProvisionedTPS((int)m_spinnerMinNumProvisionedTransactions.getValue());
        nodeSettings.setOutputCampaignArnAsVar(m_checkBoxOutputAsVariable.isSelected());
        nodeSettings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        // Check if a port object is available
        if (specs[0] == null) {
            throw new NotConfigurableException("No connection information available");
        }
        final CloudConnectionInformation connectionInformation =
            (CloudConnectionInformation)((ConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();
        // Check if the port object has connection information
        if (connectionInformation == null) {
            throw new NotConfigurableException("No connection information available");
        }
        // Fill combo box with available solution versions
        try (final AmazonPersonalizeConnection personalizeConnection =
            new AmazonPersonalizeConnection(connectionInformation)) {
            final AmazonPersonalize personalize = personalizeConnection.getClient();

            final DefaultComboBoxModel<NameArnPair> comboBoxModel =
                new DefaultComboBoxModel<NameArnPair>(AmazonPersonalizeUtils.listAllSolutionVersions(personalize)
                    .stream().map(e -> new NameArnPair(shortARN(e.getSolutionVersionArn()), e.getSolutionVersionArn()))
                    .toArray(NameArnPair[]::new));
            m_comboBoxSolutionVersionList.setModel(comboBoxModel);

            // Save the campaign names to check later if the specified name already exists
            m_campaignNames = AmazonPersonalizeUtils.listAllCampaigns(personalize).stream().map(e -> e.getName())
                .toArray(String[]::new);
        } catch (Exception e) {
            throw new NotConfigurableException(e.getMessage());
        }

        // Loading
        final AmazonPersonalizeCreateCampaignNodeSettings nodeSettings =
            new AmazonPersonalizeCreateCampaignNodeSettings();
        nodeSettings.loadSettingsForDialog(settings);
        m_textFieldCampaignName.setText(nodeSettings.getCampaignName());
        final NameArnPair solutionVersionARN = nodeSettings.getSolutionVersion();
        if (solutionVersionARN == null) {
            m_comboBoxSolutionVersionList.setSelectedItem(m_comboBoxSolutionVersionList.getItemAt(0));
        } else {
            m_comboBoxSolutionVersionList.setSelectedItem(solutionVersionARN);
        }
        m_spinnerMinNumProvisionedTransactions.setValue(nodeSettings.getMinProvisionedTPS());
        m_checkBoxOutputAsVariable.setSelected(nodeSettings.isOutputCampaignArnAsVar());
    }

    private static String shortARN(final String arn) {
        // Find index of second last occurrence of '/'
        int indexOfCut = arn.substring(0, arn.lastIndexOf('/')).lastIndexOf('/');
        return arn.substring(indexOfCut + 1);
    }

}
