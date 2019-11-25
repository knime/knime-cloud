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
 *   Apr 12, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.predict;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import org.apache.commons.lang.WordUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.mlservices.nodes.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.cloud.aws.mlservices.utils.personalize.RecipeType;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnSelectionPanel;
import org.knime.core.node.util.DataValueColumnFilter;

import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.DescribeCampaignRequest;
import com.amazonaws.services.personalize.model.DescribeRecipeRequest;
import com.amazonaws.services.personalize.model.DescribeSolutionVersionRequest;

/**
 * The abstract node dialog of the Amazon Personalize prediction nodes.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 * @param <S> the object used to transfer settings from dialog to model
 */
public abstract class AbstractAmazonPersonalizePredictNodeDialog<S extends AmazonPersonalizePredictNodeSettings>
    extends NodeDialogPane {

    /** The settings */
    protected final S m_settings = getSettings();

    private final JComboBox<NameArnPair> m_comboBoxCampaigns = new JComboBox<NameArnPair>();

    private ButtonGroup m_butonGroupMissinValues = new ButtonGroup();

    private JRadioButton m_radioButtonFail = new JRadioButton(MissingValueHandling.FAIL.getText());

    private JRadioButton m_radioButtonIgnore = new JRadioButton(MissingValueHandling.IGNORE.getText());

    /** */
    @SuppressWarnings("unchecked")
    protected final ColumnSelectionPanel m_colSelectionUserID =
        new ColumnSelectionPanel(null, new DataValueColumnFilter(StringValue.class), false);

    @SuppressWarnings("unchecked")
    private final ColumnSelectionPanel m_colSelectionItemID =
        new ColumnSelectionPanel(null, new DataValueColumnFilter(StringValue.class), !isItemIDRequired());

    /**
     *
     */
    protected AbstractAmazonPersonalizePredictNodeDialog() {
        m_butonGroupMissinValues.add(m_radioButtonFail);
        m_butonGroupMissinValues.add(m_radioButtonIgnore);

        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.weightx = 1;

        // General settings
        final JPanel panelGeneral = new JPanel(new GridBagLayout());
        panelGeneral
            .setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "General Settings"));
        final GridBagConstraints gbc2 = new GridBagConstraints();
        NodeUtils.resetGBC(gbc2);
        gbc2.fill = GridBagConstraints.NONE;
        panelGeneral.add(new JLabel("Campaign"), gbc2);
        gbc2.gridx++;
        panelGeneral.add(m_comboBoxCampaigns, gbc2);
        // User id column selection
        if (isUserIDUsed()) {
            gbc2.gridx = 0;
            gbc2.gridy++;
            panelGeneral.add(new JLabel("User ID column"), gbc2);
            gbc2.gridx++;
            gbc2.weightx = 1;
            panelGeneral.add(m_colSelectionUserID, gbc2);
        }
        // Item id column selection
        if (isItemIDUsed()) {
            gbc2.gridx = 0;
            gbc2.gridy++;
            gbc2.weightx = 0;
            panelGeneral.add(new JLabel("Item ID column" + (isItemIDRequired() ? "" : " (optional)")), gbc2);
            gbc2.gridx++;
            gbc2.weightx = 1;
            panelGeneral.add(m_colSelectionItemID, gbc2);
        }
        panel.add(panelGeneral, gbc);

        // Additional settings
        gbc.gridy++;
        final JPanel panelAdditional = layoutAdditionalSettings();
        panelAdditional.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(),
            WordUtils.capitalize(getRecipeType().toString()) + " Settings"));
        panel.add(panelAdditional, gbc);

        // Missing value settings
        gbc.gridy++;
        gbc.weighty = 1;
        final JPanel panelMissingValue = new JPanel(new GridBagLayout());
        panelMissingValue
            .setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Missing Value Handling"));
        NodeUtils.resetGBC(gbc2);
        panelMissingValue.add(m_radioButtonFail, gbc2);
        gbc2.gridy++;
        gbc2.weightx = 1;
        gbc2.weighty = 1;
        gbc2.fill = GridBagConstraints.NONE;
        panelMissingValue.add(m_radioButtonIgnore, gbc2);

        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(panelMissingValue, gbc);

        addTab("Options", panel);
    }

    /**
     * Create and fill additional settings panel for the dialog. May be overridden by subclasses.
     *
     * @return The panel for the dialog
     */
    protected JPanel layoutAdditionalSettings() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        if (isUserIDUsed() && isItemIDUsed()
            && m_colSelectionUserID.getSelectedColumn().equals(m_colSelectionItemID.getSelectedColumn())) {
            throw new InvalidSettingsException("The user ID and item ID columns must not be the same.");
        }
        m_settings.setCampaign((NameArnPair)m_comboBoxCampaigns.getSelectedItem());
        m_settings.setUserIDCol(m_colSelectionUserID.getSelectedColumn());
        m_settings.setItemIDCol(m_colSelectionItemID.getSelectedColumn());
        m_settings.setMissingValueHandling(
            m_radioButtonFail.isSelected() ? MissingValueHandling.FAIL : MissingValueHandling.IGNORE);

        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)specs[0];
        final CloudConnectionInformation connectionInformation =
            (CloudConnectionInformation)object.getConnectionInformation();
        // Check if the port object has connection information
        if (connectionInformation == null) {
            throw new NotConfigurableException("No connection information available");
        }
        // List all existing campaigns
        try (final AmazonPersonalizeConnection personalizeConnection =
            new AmazonPersonalizeConnection(connectionInformation)) {
            final AmazonPersonalize personalizeClient = personalizeConnection.getClient();

            // Filter only the campaigns that have a solution with the proper recipe type
            final DefaultComboBoxModel<NameArnPair> comboBoxModel = new DefaultComboBoxModel<NameArnPair>(
                AmazonPersonalizeUtils.listAllCampaigns(personalizeClient).stream().filter(e -> {
                    final String recipeType =
                        personalizeClient
                            .describeRecipe(
                                new DescribeRecipeRequest().withRecipeArn(personalizeClient
                                    .describeSolutionVersion(
                                        new DescribeSolutionVersionRequest().withSolutionVersionArn(personalizeClient
                                            .describeCampaign(
                                                new DescribeCampaignRequest().withCampaignArn(e.getCampaignArn()))
                                            .getCampaign().getSolutionVersionArn()))
                                    .getSolutionVersion().getRecipeArn()))
                            .getRecipe().getRecipeType();
                    return recipeType.equals(getRecipeType().getType());
                }).map(e -> new NameArnPair(e.getName(), e.getCampaignArn())).toArray(NameArnPair[]::new));
            m_comboBoxCampaigns.setModel(comboBoxModel);
            if (comboBoxModel.getSize() == 0) {
                throw new NotConfigurableException("No campaign of type '" + getRecipeType().toString()
                    + "' found. You can create one using the 'Amazon Personalize Create Campaign' node.");
            }
        } catch (Exception e) {
            throw new NotConfigurableException(e.getMessage());
        }

        // Loading
        final DataTableSpec spec = (DataTableSpec)specs[1];
        m_settings.loadSettingsForDialog(settings);
        final NameArnPair campaign = m_settings.getCampaign();
        if (campaign != null) {
            m_comboBoxCampaigns.setSelectedItem(campaign);
        } else {
            m_comboBoxCampaigns.setSelectedItem(m_comboBoxCampaigns.getItemAt(0));
        }
        m_colSelectionUserID.update(spec, m_settings.getUserIDCol());
        m_colSelectionItemID.update(spec, m_settings.getItemIDCol());
        m_radioButtonFail.setSelected(m_settings.getMissingValueHandling() == MissingValueHandling.FAIL);
        m_radioButtonIgnore.setSelected(m_settings.getMissingValueHandling() == MissingValueHandling.IGNORE);
    }

    /**
     * @return the settings
     */
    protected abstract S getSettings();

    /**
     * @return true, if user id can be used
     */
    protected abstract boolean isUserIDUsed();

    /**
     * @return true, if item id can be used
     */
    protected abstract boolean isItemIDUsed();

    /**
     * @return true, if item id is required
     */
    protected abstract boolean isItemIDRequired();

    /**
     * @return the recipe type
     */
    protected abstract RecipeType getRecipeType();

}
