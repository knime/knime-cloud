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
package org.knime.cloud.aws.mlservices.nodes.personalize.train;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.mlservices.nodes.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.cloud.aws.mlservices.utils.personalize.RecipeType;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.services.personalize.model.DatasetGroupSummary;
import com.amazonaws.services.personalize.model.DescribeRecipeRequest;
import com.amazonaws.services.personalize.model.Recipe;
import com.amazonaws.services.personalize.model.RecipeSummary;

/**
 * Node dialog for Amazon Personalize solution version creator node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeCreateSolutionVersionNodeDialog extends NodeDialogPane {

    private final ButtonGroup m_buttonGroupCreateNewSolution = new ButtonGroup();

    private final JRadioButton m_radioButtonCreateNewSolution =
        new JRadioButton("Create a new solution and a version of it");

    private final JRadioButton m_radioButtonUseExistingSolution =
        new JRadioButton("Create a new version of an existing solution");

    private final JComboBox<NameArnPair> m_comboBoxExistingSolutions = new JComboBox<>();

    private final JComboBox<NameArnPair> m_comboBoxDatasetGroupList = new JComboBox<>();

    private final JTextField m_textFieldPrefixSolutionName = new JTextField();

    private final JCheckBox m_checkBoxOutputSolutionVersionARNAsVar =
        new JCheckBox("Provide the solution version ARN as flow variable");

    private final ButtonGroup m_buttonGroupRecipe = new ButtonGroup();

    private final JRadioButton m_radioButtonPredefinedRecipe = new JRadioButton(RecipeSelection.PREDEFINED.getText());

    private final JRadioButton m_radioButtonUserDefinedRecipe =
        new JRadioButton(RecipeSelection.USER_DEFINED.getText());

    private final JRadioButton m_radioButtonAutoML = new JRadioButton(RecipeSelection.AUTOML.getText());

    private final ButtonGroup m_buttonGroupRecipeType = new ButtonGroup();

    private final JRadioButton m_radioButtonUserPersonalization =
        new JRadioButton(RecipeType.USER_PERSONALIZATION.toString());

    private final JRadioButton m_radioButtonPersonalizedRanking =
        new JRadioButton(RecipeType.PERSONALIZED_RANKING.toString());

    private final JRadioButton m_radioButtonRelatedItems = new JRadioButton(RecipeType.RELATED_ITEMS.toString());

    private final JComboBox<NameArnPair> m_comboBoxUserPersonalizationRecipeList = new JComboBox<>();

    private final JComboBox<NameArnPair> m_comboBoxPersonalizedRankingRecipeList = new JComboBox<>();

    private final JComboBox<NameArnPair> m_comboBoxRelatedItemsRecipeList = new JComboBox<>();

    private final JLabel m_labelUserDefinedRecipeArn = new JLabel("Recipe ARN");

    private final JTextField m_textFieldUserDefinedRecipeArn = new JTextField();

    private final JCheckBox m_checkBoxHyperParamOpt = new JCheckBox("Perform hyperparameter optimization");

    private JPanel m_createNewSolutionPanel;

    private JPanel m_useExistingSolutionPanel;

    AmazonPersonalizeCreateSolutionVersionNodeDialog() {

        m_buttonGroupCreateNewSolution.add(m_radioButtonCreateNewSolution);
        m_buttonGroupCreateNewSolution.add(m_radioButtonUseExistingSolution);
        m_radioButtonCreateNewSolution.addChangeListener(l -> {
            enablePanelComponents(m_createNewSolutionPanel, m_radioButtonCreateNewSolution.isSelected());
            enablePanelComponents(m_useExistingSolutionPanel, !m_radioButtonCreateNewSolution.isSelected());
            enableComponents();
        });

        m_buttonGroupRecipe.add(m_radioButtonPredefinedRecipe);
        m_buttonGroupRecipe.add(m_radioButtonUserDefinedRecipe);
        m_buttonGroupRecipe.add(m_radioButtonAutoML);
        m_radioButtonPredefinedRecipe.addChangeListener(l -> enableComponents());
        m_radioButtonUserDefinedRecipe.addChangeListener(l -> enableComponents());
        m_radioButtonAutoML.addChangeListener(l -> enableComponents());

        m_buttonGroupRecipeType.add(m_radioButtonUserPersonalization);
        m_buttonGroupRecipeType.add(m_radioButtonPersonalizedRanking);
        m_buttonGroupRecipeType.add(m_radioButtonRelatedItems);
        m_radioButtonUserPersonalization.addChangeListener(l -> enableComponents());
        m_radioButtonPersonalizedRanking.addChangeListener(l -> enableComponents());
        m_radioButtonRelatedItems.addChangeListener(l -> enableComponents());

        addTab("Options", initLayout());
    }

    /**
     * Create and fill panel for the dialog.
     *
     * @return The panel for the dialog
     */
    private JPanel initLayout() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        panel.add(getGeneralSettingsPanel(), gbc);

        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridy++;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(getOutputSettingsPanel(), gbc);

        return panel;
    }

    private JPanel getGeneralSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "General settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        panel.add(m_radioButtonCreateNewSolution, gbc);
        gbc.gridy++;
        gbc.insets = new Insets(5, 30, 5, 5);
        m_createNewSolutionPanel = getCreateNewSolutionPanel();
        panel.add(m_createNewSolutionPanel, gbc);
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(m_radioButtonUseExistingSolution, gbc);
        gbc.gridy++;
        gbc.weightx = 1;
        gbc.insets = new Insets(5, 30, 5, 5);
        m_useExistingSolutionPanel = getUseExistingSolutionPanel();
        panel.add(m_useExistingSolutionPanel, gbc);
        return panel;
    }

    private JPanel getOutputSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.weightx = 1;
        panel.add(m_checkBoxOutputSolutionVersionARNAsVar, gbc);
        return panel;
    }

    private JPanel getUseExistingSolutionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createEtchedBorder());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        panel.add(new JLabel("Solution"), gbc);
        gbc.fill = GridBagConstraints.NONE;
        gbc.weighty = 1;
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_comboBoxExistingSolutions, gbc);

        return panel;
    }

    private JPanel getCreateNewSolutionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createEtchedBorder());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.weightx = 1;

        // General settings
        final JPanel panelGeneral = new JPanel(new GridBagLayout());
        panelGeneral
            .setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Solution settings"));
        final GridBagConstraints gbc2 = new GridBagConstraints();
        NodeUtils.resetGBC(gbc2);
        panelGeneral.add(new JLabel("Dataset group"), gbc2);
        gbc2.gridx++;
        panelGeneral.add(m_comboBoxDatasetGroupList, gbc2);
        gbc2.gridx = 0;
        gbc2.gridy++;
        panelGeneral.add(new JLabel("Solution name"), gbc2);
        gbc2.gridx++;
        gbc2.gridwidth = 2;
        gbc2.weightx = 1;
        panelGeneral.add(m_textFieldPrefixSolutionName, gbc2);
        gbc2.gridy++;
        gbc2.gridx = 0;
        gbc2.weightx = 1;
        gbc2.gridwidth = 3;
        gbc2.weightx = 1;
        panelGeneral.add(m_checkBoxHyperParamOpt, gbc2);
        panel.add(panelGeneral, gbc);

        // Recipe selection
        gbc.gridy++;
        panel.add(getRecipeSelectionPanel(), gbc);

        return panel;
    }

    private JPanel getRecipeSelectionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(new TitledBorder(new EtchedBorder(), "Recipe selection"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        // Predefined recipe
        panel.add(m_radioButtonPredefinedRecipe, gbc);
        final JPanel panelRecipeTypeSelection = new JPanel(new GridBagLayout());
        panelRecipeTypeSelection.setBorder(BorderFactory.createEtchedBorder());
        final GridBagConstraints gbc2 = new GridBagConstraints();
        NodeUtils.resetGBC(gbc2);
        panelRecipeTypeSelection.add(m_radioButtonUserPersonalization, gbc2);
        gbc2.gridx++;
        panelRecipeTypeSelection.add(m_comboBoxUserPersonalizationRecipeList, gbc2);
        gbc2.gridy++;
        gbc2.gridx = 0;
        panelRecipeTypeSelection.add(m_radioButtonPersonalizedRanking, gbc2);
        gbc2.gridx++;
        panelRecipeTypeSelection.add(m_comboBoxPersonalizedRankingRecipeList, gbc2);
        gbc2.gridy++;
        gbc2.gridx = 0;
        panelRecipeTypeSelection.add(m_radioButtonRelatedItems, gbc2);
        gbc2.gridx++;
        panelRecipeTypeSelection.add(m_comboBoxRelatedItemsRecipeList, gbc2);
        gbc2.gridx++;
        gbc2.weightx = 1;
        // Dummy label to keep combo boxes left and all same size
        panelRecipeTypeSelection.add(new JLabel(), gbc2);
        gbc.gridy++;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(5, 30, 5, 5);
        panel.add(panelRecipeTypeSelection, gbc);

        // AutoML recipe
        gbc.weightx = 1;
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(5, 5, 5, 5);
        panel.add(m_radioButtonAutoML, gbc);

        return panel;
    }

    private void enableComponents() {
        if (m_comboBoxExistingSolutions.getModel().getSize() == 0) {
            m_comboBoxExistingSolutions.setEnabled(false);
            m_radioButtonUseExistingSolution.setEnabled(false);
            m_radioButtonCreateNewSolution.setSelected(true);
        } else {
            m_radioButtonUseExistingSolution.setEnabled(true);
        }
        boolean selected = m_radioButtonCreateNewSolution.isSelected();
        m_comboBoxExistingSolutions.setEnabled(!selected);
        if (selected) {
            m_radioButtonUserPersonalization.setEnabled(m_radioButtonPredefinedRecipe.isSelected());
            m_radioButtonPersonalizedRanking.setEnabled(m_radioButtonPredefinedRecipe.isSelected());
            m_radioButtonRelatedItems.setEnabled(m_radioButtonPredefinedRecipe.isSelected());
            m_comboBoxUserPersonalizationRecipeList.setEnabled(
                m_radioButtonUserPersonalization.isEnabled() && m_radioButtonUserPersonalization.isSelected());
            m_comboBoxPersonalizedRankingRecipeList.setEnabled(
                m_radioButtonPersonalizedRanking.isEnabled() && m_radioButtonPersonalizedRanking.isSelected());
            m_comboBoxRelatedItemsRecipeList
                .setEnabled(m_radioButtonRelatedItems.isEnabled() && m_radioButtonRelatedItems.isSelected());

            m_labelUserDefinedRecipeArn.setEnabled(m_radioButtonUserDefinedRecipe.isSelected());
            m_textFieldUserDefinedRecipeArn.setEnabled(m_radioButtonUserDefinedRecipe.isSelected());
        }
    }

    private static void enablePanelComponents(final JPanel panel, final boolean enabled) {
        for (final Component c : panel.getComponents()) {
            if (c instanceof JPanel) {
                enablePanelComponents((JPanel)c, enabled);
            }
            c.setEnabled(enabled);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        final AmazonPersonalizeCreateSolutionVersionNodeSettings nodeSettings =
            new AmazonPersonalizeCreateSolutionVersionNodeSettings();
        nodeSettings.setCreateNewSolution(m_radioButtonCreateNewSolution.isSelected());
        nodeSettings.setDatasetGroup((NameArnPair)m_comboBoxDatasetGroupList.getSelectedItem());
        if (m_radioButtonPredefinedRecipe.isSelected()) {
            nodeSettings.setRecipeSelection(RecipeSelection.PREDEFINED);
        } else if (m_radioButtonUserDefinedRecipe.isSelected()) {
            nodeSettings.setRecipeSelection(RecipeSelection.USER_DEFINED);
        } else {
            nodeSettings.setRecipeSelection(RecipeSelection.AUTOML);
        }
        if (m_radioButtonUserPersonalization.isSelected()) {
            nodeSettings.setPredefinedRecipeType(RecipeType.USER_PERSONALIZATION);
            nodeSettings.setPredefinedRecipe((NameArnPair)m_comboBoxUserPersonalizationRecipeList.getSelectedItem());
        } else if (m_radioButtonPersonalizedRanking.isSelected()) {
            nodeSettings.setPredefinedRecipeType(RecipeType.PERSONALIZED_RANKING);
            nodeSettings.setPredefinedRecipe((NameArnPair)m_comboBoxPersonalizedRankingRecipeList.getSelectedItem());
        } else {
            nodeSettings.setPredefinedRecipeType(RecipeType.RELATED_ITEMS);
            nodeSettings.setPredefinedRecipe((NameArnPair)m_comboBoxRelatedItemsRecipeList.getSelectedItem());
        }
        nodeSettings.setUserDefinedRecipeArn(m_textFieldUserDefinedRecipeArn.getText());
        nodeSettings.setHyperparameterOpt(m_checkBoxHyperParamOpt.isSelected());
        nodeSettings.setSolutionName(m_textFieldPrefixSolutionName.getText());
        nodeSettings.setOutputSolutionVersionArnAsVar(m_checkBoxOutputSolutionVersionARNAsVar.isSelected());
        nodeSettings.setExistingSolution((NameArnPair)m_comboBoxExistingSolutions.getSelectedItem());

        nodeSettings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        if (specs[0] == null) {
            throw new NotConfigurableException("No connection information available");
        }
        final CloudConnectionInformation connectionInformation =
            (CloudConnectionInformation)((ConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();
        // Check if the port object has connection information
        if (connectionInformation == null) {
            throw new NotConfigurableException("No connection information available");
        }
        // List all existing dataset groups and solutions
        try (final AmazonPersonalizeConnection personalizeConnection =
            new AmazonPersonalizeConnection(connectionInformation)) {
            final AmazonPersonalize personalizeClient = personalizeConnection.getClient();
            final List<DatasetGroupSummary> listAllDatasetGroups =
                AmazonPersonalizeUtils.listAllDatasetGroups(personalizeClient);
            if (listAllDatasetGroups.size() == 0) {
                throw new NotConfigurableException("No existing dataset group found. Upload datasets first.");
            }
            final DefaultComboBoxModel<NameArnPair> comboBoxModel =
                new DefaultComboBoxModel<NameArnPair>(listAllDatasetGroups.stream()
                    .map(e -> NameArnPair.of(e.getName(), e.getDatasetGroupArn())).toArray(NameArnPair[]::new));
            m_comboBoxDatasetGroupList.setModel(comboBoxModel);

            final DefaultComboBoxModel<NameArnPair> comboBoxModel2 =
                new DefaultComboBoxModel<NameArnPair>(AmazonPersonalizeUtils.listAllSolutions(personalizeClient)
                    .stream().map(e -> NameArnPair.of(e.getName(), e.getSolutionArn())).toArray(NameArnPair[]::new));
            m_comboBoxExistingSolutions.setModel(comboBoxModel2);

            final DefaultComboBoxModel<NameArnPair> comboBoxModelUserPersonalization = new DefaultComboBoxModel<>();
            final DefaultComboBoxModel<NameArnPair> comboBoxModelPersonalizedRanking = new DefaultComboBoxModel<>();
            final DefaultComboBoxModel<NameArnPair> comboBoxModelRelatedItems = new DefaultComboBoxModel<>();
            for (RecipeSummary rs : AmazonPersonalizeUtils.listAllRecipes(personalizeClient)) {
                final Recipe recipe = personalizeClient
                    .describeRecipe(new DescribeRecipeRequest().withRecipeArn(rs.getRecipeArn())).getRecipe();
                final NameArnPair nameArnPair = NameArnPair.of(recipe.getName(), recipe.getRecipeArn());
                switch (RecipeType.ofRecipeType(recipe)) {
                    case USER_PERSONALIZATION:
                        comboBoxModelUserPersonalization.addElement(nameArnPair);
                        break;
                    case PERSONALIZED_RANKING:
                        comboBoxModelPersonalizedRanking.addElement(nameArnPair);
                        break;
                    case RELATED_ITEMS:
                        comboBoxModelRelatedItems.addElement(nameArnPair);
                        break;
                }
            }
            m_comboBoxUserPersonalizationRecipeList.setModel(comboBoxModelUserPersonalization);
            m_comboBoxPersonalizedRankingRecipeList.setModel(comboBoxModelPersonalizedRanking);
            m_comboBoxRelatedItemsRecipeList.setModel(comboBoxModelRelatedItems);

        } catch (Exception e) {
            throw new NotConfigurableException(e.getMessage());
        }
        // Loading
        final AmazonPersonalizeCreateSolutionVersionNodeSettings nodeSettings =
            new AmazonPersonalizeCreateSolutionVersionNodeSettings();
        nodeSettings.loadSettingsForDialog(settings);
        m_radioButtonCreateNewSolution.setSelected(nodeSettings.isCreateNewSolution());
        m_radioButtonUseExistingSolution.setSelected(!nodeSettings.isCreateNewSolution());
        final NameArnPair datasetGroup = nodeSettings.getDatasetGroup();
        if (datasetGroup == null) {
            m_comboBoxDatasetGroupList.setSelectedIndex(0);
        } else {
            m_comboBoxDatasetGroupList.setSelectedItem(datasetGroup);
        }
        final RecipeSelection recipeSelection = nodeSettings.getRecipeSelection();
        m_radioButtonPredefinedRecipe.setSelected(recipeSelection == RecipeSelection.PREDEFINED);
        m_radioButtonUserDefinedRecipe.setSelected(recipeSelection == RecipeSelection.USER_DEFINED);
        m_radioButtonAutoML.setSelected(recipeSelection == RecipeSelection.AUTOML);
        final RecipeType predefinedRecipeType = nodeSettings.getPredefinedRecipeType();
        final NameArnPair predefinedRecipe = nodeSettings.getPredefinedRecipe();
        if (predefinedRecipeType == RecipeType.USER_PERSONALIZATION) {
            m_radioButtonUserPersonalization.setSelected(true);
            if (predefinedRecipe != null) {
                m_comboBoxUserPersonalizationRecipeList.setSelectedItem(predefinedRecipe);
            }
        }
        if (predefinedRecipeType == RecipeType.PERSONALIZED_RANKING) {
            m_radioButtonPersonalizedRanking.setSelected(true);
            m_comboBoxPersonalizedRankingRecipeList.setSelectedItem(predefinedRecipe);
        }
        if (predefinedRecipeType == RecipeType.RELATED_ITEMS) {
            m_radioButtonRelatedItems.setSelected(true);
            m_comboBoxRelatedItemsRecipeList.setSelectedItem(predefinedRecipe);
        }
        m_textFieldUserDefinedRecipeArn.setText(nodeSettings.getUserDefinedRecipeArn());
        m_checkBoxHyperParamOpt.setSelected(nodeSettings.isHyperparameterOpt());
        m_textFieldPrefixSolutionName.setText(nodeSettings.getSolutionName());
        m_checkBoxOutputSolutionVersionARNAsVar.setSelected(nodeSettings.isOutputSolutionVersionArnAsVar());
        final NameArnPair existingSolution = nodeSettings.getExistingSolution();
        if (existingSolution == null && m_comboBoxExistingSolutions.getModel().getSize() > 0) {
            m_comboBoxExistingSolutions.setSelectedIndex(0);
        } else {
            m_comboBoxExistingSolutions.setSelectedItem(existingSolution);
        }

        enablePanelComponents(m_createNewSolutionPanel, !m_radioButtonUseExistingSolution.isSelected());
        enablePanelComponents(m_useExistingSolutionPanel, m_radioButtonUseExistingSolution.isSelected());
        enableComponents();
    }
}
