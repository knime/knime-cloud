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
 *   Oct 28, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.upload;

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.cloud.aws.mlservices.nodes.personalize.AmazonPersonalizeConnection;
import org.knime.cloud.aws.mlservices.utils.personalize.AmazonPersonalizeUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.core.node.workflow.FlowVariable;

import com.amazonaws.services.identitymanagement.model.AmazonIdentityManagementException;
import com.amazonaws.services.personalize.AmazonPersonalize;
import com.amazonaws.util.StringUtils;

/**
 * Abstract node dialog for Amazon Personalize data upload nodes.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 * @param <S> the object used to transfer settings from dialog to model
 */
public abstract class AbstractAmazonPersonalizeDataUploadNodeDialog<S extends AbstractAmazonPersonalizeDataUploadNodeSettings>
    extends NodeDialogPane {

    /** The settings */
    protected final S m_settings = createSettings();

    private CloudConnectionInformation m_connectionInformation;

    private RemoteFileChooserPanel m_fileChooserTarget;

    private final ButtonGroup m_buttonGroupRoleSelection = new ButtonGroup();

    private final JRadioButton m_radioButtonCustomRole = new JRadioButton("Enter custom IAM role ARN");

    private final JRadioButton m_radioButtonAvailableRole = new JRadioButton("Select IAM role ARN");

    private final JLabel m_labelNoListRolePermissions;

    private final JComboBox<String> m_comboBoxRoleList = new JComboBox<>();

    private final JTextField m_textFieldCustomRole = new JTextField();

    private final ButtonGroup m_buttonGroupDatasetGroup = new ButtonGroup();

    private final JRadioButton m_radioButtonUploadToNewDatasetGroup = new JRadioButton("Upload to new dataset group");

    private final JRadioButton m_radioButtonUploadToExistingDatasetGroup =
        new JRadioButton("Upload to existing dataset group");

    private final JTextField m_textFieldDatasetGroupName = new JTextField("new-dataset-group");

    private final JComboBox<String> m_comboBoxDatasetGroupList = new JComboBox<>();

    private final ButtonGroup m_buttonGroupOverwriteDataset = new ButtonGroup();

    private final JRadioButton m_radioButtonOverwriteExistingDataset =
        new JRadioButton(OverwritePolicy.OVERWRITE.getName());

    private final JRadioButton m_radioButtonAbortOnExistingDataset = new JRadioButton(OverwritePolicy.ABORT.getName());

    private final JTextField m_textFieldDatasetName = new JTextField("users-dataset");

    private final JTextField m_textFieldImportJobNamePrefix = new JTextField();

    private final JTextField m_textFieldSchemaNamePrefix = new JTextField();

    /** The filter panel which needs to hide selected required columns. */
    protected final DataColumnSpecFilterPanel m_columnFilterPanel = new DataColumnSpecFilterPanel();

    /** The input table spec */
    protected DataTableSpec m_spec;

    /** */
    @SuppressWarnings({"deprecation"})
    protected AbstractAmazonPersonalizeDataUploadNodeDialog() {
        m_fileChooserTarget =
            new RemoteFileChooserPanel(getPanel(), "Target on S3", true, "targetHistory", RemoteFileChooser.SELECT_DIR,
                createFlowVariableModel("target", FlowVariable.Type.STRING), m_connectionInformation);

        m_labelNoListRolePermissions =
            new JLabel("You don't have permission to list the existing roles. Enter a custome role arn.");
        m_labelNoListRolePermissions.setForeground(Color.RED);
        m_labelNoListRolePermissions.setFont(new Font(m_labelNoListRolePermissions.getFont().getName(), Font.ITALIC,
            m_labelNoListRolePermissions.getFont().getSize()));

        m_buttonGroupRoleSelection.add(m_radioButtonCustomRole);
        m_buttonGroupRoleSelection.add(m_radioButtonAvailableRole);
        m_radioButtonCustomRole.addChangeListener(l -> enableComponents());
        m_radioButtonAvailableRole.addChangeListener(l -> enableComponents());

        m_buttonGroupDatasetGroup.add(m_radioButtonUploadToExistingDatasetGroup);
        m_buttonGroupDatasetGroup.add(m_radioButtonUploadToNewDatasetGroup);
        m_radioButtonUploadToExistingDatasetGroup.addChangeListener(l -> enableComponents());
        m_radioButtonUploadToNewDatasetGroup.addChangeListener(l -> enableComponents());

        m_buttonGroupOverwriteDataset.add(m_radioButtonOverwriteExistingDataset);
        m_buttonGroupOverwriteDataset.add(m_radioButtonAbortOnExistingDataset);

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

        // S3 file chooser
        NodeUtils.resetGBC(gbc);
        gbc.gridwidth = 2;
        gbc.weightx = 1;
        panel.add(m_fileChooserTarget.getPanel(), gbc);

        // IAM service role selection
        final JPanel panelRoleSelection = new JPanel(new GridBagLayout());
        panelRoleSelection.setBorder(new TitledBorder(new EtchedBorder(), "IAM service role"));
        final GridBagConstraints gbcRoleSelection = new GridBagConstraints();
        NodeUtils.resetGBC(gbcRoleSelection);
        gbcRoleSelection.gridwidth = 2;
        panelRoleSelection.add(m_labelNoListRolePermissions, gbcRoleSelection);
        gbcRoleSelection.gridy++;
        gbcRoleSelection.gridwidth = 1;
        panelRoleSelection.add(m_radioButtonCustomRole, gbcRoleSelection);
        gbcRoleSelection.gridx++;
        panelRoleSelection.add(m_textFieldCustomRole, gbcRoleSelection);
        gbcRoleSelection.gridy++;
        gbcRoleSelection.gridx = 0;
        panelRoleSelection.add(m_radioButtonAvailableRole, gbcRoleSelection);
        gbcRoleSelection.gridx++;
        gbcRoleSelection.weightx = 1;
        panelRoleSelection.add(m_comboBoxRoleList, gbcRoleSelection);
        gbc.gridy++;
        panel.add(panelRoleSelection, gbc);

        // Dataset group selection
        final JPanel panelDatasetGroupSelection = new JPanel(new GridBagLayout());
        panelDatasetGroupSelection.setBorder(new TitledBorder(new EtchedBorder(), "Dataset group"));
        final GridBagConstraints gbcDatasetGroupSelection = new GridBagConstraints();
        NodeUtils.resetGBC(gbcDatasetGroupSelection);
        panelDatasetGroupSelection.add(m_radioButtonUploadToNewDatasetGroup, gbcDatasetGroupSelection);
        gbcDatasetGroupSelection.gridx++;
        panelDatasetGroupSelection.add(m_textFieldDatasetGroupName, gbcDatasetGroupSelection);
        gbcDatasetGroupSelection.gridy++;
        gbcDatasetGroupSelection.gridx = 0;
        panelDatasetGroupSelection.add(m_radioButtonUploadToExistingDatasetGroup, gbcDatasetGroupSelection);
        gbcDatasetGroupSelection.gridx++;
        gbcDatasetGroupSelection.weightx = 1;
        panelDatasetGroupSelection.add(m_comboBoxDatasetGroupList, gbcDatasetGroupSelection);
        gbc.gridy++;
        panel.add(panelDatasetGroupSelection, gbc);

        // Column mapping (required)
        gbc.gridy++;
        gbc.gridx = 0;
        final JPanel panelColumnMapping = layoutRequiredColumnMapping();
        panelColumnMapping.setBorder(new TitledBorder(new EtchedBorder(), "Required columns"));
        panel.add(panelColumnMapping, gbc);

        // Column mapping (required)
        final JPanel panelOptionalColumnMapping = layoutOptionalColumnMapping();
        if (panelOptionalColumnMapping != null) {
            gbc.gridy++;
            gbc.gridx = 0;
            panelOptionalColumnMapping.setBorder(new TitledBorder(new EtchedBorder(), "Optional columns"));
            panel.add(panelOptionalColumnMapping, gbc);
        }

        // Metadata columns filter panel
        gbc.gridy++;
        gbc.weighty = 1;
        m_columnFilterPanel.setBorder(new TitledBorder(new EtchedBorder(), "Metadata columns"));
        panel.add(m_columnFilterPanel, gbc);

        final JPanel panelAdditionalSettings = new JPanel(new GridBagLayout());
        panelAdditionalSettings.setBorder(new TitledBorder(new EtchedBorder(), "Additional settings"));
        final GridBagConstraints gbcAdditionalSettings = new GridBagConstraints();
        NodeUtils.resetGBC(gbcAdditionalSettings);
        // Dataset name
        panelAdditionalSettings.add(new JLabel("Dataset name"), gbcAdditionalSettings);
        gbcAdditionalSettings.gridx++;
        panelAdditionalSettings.add(m_textFieldDatasetName, gbcAdditionalSettings);
        // Import job name prefix
        gbcAdditionalSettings.gridy++;
        gbcAdditionalSettings.gridx = 0;
        panelAdditionalSettings.add(new JLabel("Prefix of import job name"), gbcAdditionalSettings);
        gbcAdditionalSettings.gridx++;
        gbcAdditionalSettings.weightx = 1;
        panelAdditionalSettings.add(m_textFieldImportJobNamePrefix, gbcAdditionalSettings);
        // Import job name prefix
        gbcAdditionalSettings.gridy++;
        gbcAdditionalSettings.gridx = 0;
        gbcAdditionalSettings.weightx = 0;
        panelAdditionalSettings.add(new JLabel("Prefix of schema name"), gbcAdditionalSettings);
        gbcAdditionalSettings.gridx++;
        gbcAdditionalSettings.weightx = 1;
        panelAdditionalSettings.add(m_textFieldSchemaNamePrefix, gbcAdditionalSettings);
        // Overwrite policy
        final JPanel overwritePolicyPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbcOverwritePolicy = new GridBagConstraints();
        NodeUtils.resetGBC(gbcOverwritePolicy);
        gbcOverwritePolicy.insets = new Insets(0, 0, 0, 0);
        overwritePolicyPanel.add(m_radioButtonOverwriteExistingDataset, gbcOverwritePolicy);
        gbcOverwritePolicy.gridx++;
        gbcOverwritePolicy.weightx = 1;
        overwritePolicyPanel.add(m_radioButtonAbortOnExistingDataset, gbcOverwritePolicy);
        overwritePolicyPanel.setBorder(new TitledBorder(new EtchedBorder(), "If the dataset group already contains an "
            + StringUtils.lowerCase(m_settings.getDatasetType()) + " dataset..."));
        gbc.gridy++;
        gbc.gridx = 0;
        panelAdditionalSettings.add(overwritePolicyPanel, gbc);
        gbc.gridy++;
        gbc.weighty = 0;
        panel.add(panelAdditionalSettings, gbc);
        return panel;
    }

    /**
     * @return panel with required column mapping
     */
    protected abstract JPanel layoutRequiredColumnMapping();

    /**
     * @return panel with optional column mapping
     */
    protected JPanel layoutOptionalColumnMapping() {
        return null;
    }

    private void enableComponents() {
        m_textFieldCustomRole.setEnabled(m_radioButtonCustomRole.isSelected());
        m_comboBoxRoleList
            .setEnabled(m_radioButtonAvailableRole.isSelected() && m_radioButtonAvailableRole.isEnabled());

        m_textFieldDatasetGroupName.setEnabled(m_radioButtonUploadToNewDatasetGroup.isSelected());
        m_comboBoxDatasetGroupList.setEnabled(m_radioButtonUploadToExistingDatasetGroup.isSelected());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setTarget(m_fileChooserTarget.getSelection());

        if (m_radioButtonUploadToNewDatasetGroup.isSelected()) {
            m_settings.setSelectedDatasetGroup(m_textFieldDatasetGroupName.getText());
        } else {
            m_settings.setSelectedDatasetGroup((String)m_comboBoxDatasetGroupList.getSelectedItem());
        }
        m_settings.setDatasetName(m_textFieldDatasetName.getText());
        if (m_radioButtonOverwriteExistingDataset.isSelected()) {
            m_settings.setOverwriteDatasetPolicy(OverwritePolicy.OVERWRITE.toString());
        } else {
            m_settings.setOverwriteDatasetPolicy(OverwritePolicy.ABORT.toString());
        }
        m_settings.setPrefixImportJobName(m_textFieldImportJobNamePrefix.getText());
        m_settings.setPrefixSchemaName(m_textFieldSchemaNamePrefix.getText());
        if (m_radioButtonCustomRole.isSelected()) {
            m_settings.setIamServiceRoleArn(m_textFieldCustomRole.getText().trim());
        } else {
            m_settings.setIamServiceRoleArn((String)m_comboBoxRoleList.getSelectedItem());
        }

        m_columnFilterPanel.saveConfiguration(m_settings.getFilterConfig());
        // Check metadata column selection
        final int numOptionalColumns = m_settings.getNumSelectedOptionalColumns();
        final int maxMetadataColumns = m_settings.getMaxMetadataColumns();
        final int numIncludes = m_settings.getFilterConfig().applyTo(m_spec).getIncludes().length
            - m_settings.getNumRequiredColumns() - numOptionalColumns;
        if (numIncludes < m_settings.getMinMetadataColumns()) {
            throw new InvalidSettingsException(
                "At least " + m_settings.getMinMetadataColumns() + " metadata columns must be included.");
        }
        if (numIncludes + numOptionalColumns > maxMetadataColumns) {
            if (numOptionalColumns > 0) {
                throw new InvalidSettingsException(
                    "At most " + maxMetadataColumns + " optional and metadata columns must be selected.");
            }
            throw new InvalidSettingsException("At most " + maxMetadataColumns + " metadata columns must be included.");
        }

        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        // Check if a port object is available
        if (specs[0] == null) {
            throw new NotConfigurableException("No connection information available.");
        }
        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)specs[0];
        m_connectionInformation = (CloudConnectionInformation)object.getConnectionInformation();
        // Check if the port object has connection information
        if (m_connectionInformation == null) {
            throw new NotConfigurableException("No connection information available.");
        }
        m_fileChooserTarget.setConnectionInformation(m_connectionInformation);

        // Try to list all available roles
        try {
            final DefaultComboBoxModel<String> comboBoxModel = new DefaultComboBoxModel<String>(AmazonPersonalizeUtils
                .listAllRoles(m_connectionInformation).stream().map(e -> e.getArn()).toArray(String[]::new));
            m_comboBoxRoleList.setModel(comboBoxModel);
            m_labelNoListRolePermissions.setVisible(false);
            m_radioButtonAvailableRole.setEnabled(true);
        } catch (Exception e) {
            if (!(e instanceof AmazonIdentityManagementException)) {
                throw new NotConfigurableException(e.getMessage());
            }
            // AmazonIdentityManagementException happens if the user does not have permissions to list roles
            m_radioButtonAvailableRole.setEnabled(false);
        }

        // List all existing dataset groups
        try (final AmazonPersonalizeConnection personalizeConnection =
            new AmazonPersonalizeConnection(m_connectionInformation)) {
            final AmazonPersonalize personalize = personalizeConnection.getClient();
            final DefaultComboBoxModel<String> comboBoxModel = new DefaultComboBoxModel<String>(AmazonPersonalizeUtils
                .listAllDatasetGroups(personalize).stream().map(e -> e.getName()).toArray(String[]::new));
            m_comboBoxDatasetGroupList.setModel(comboBoxModel);
        } catch (Exception e) {
            throw new NotConfigurableException(e.getMessage());
        }

        // Load settings
        m_spec = (DataTableSpec)specs[1];
        m_settings.loadSettingsForDialog(settings, m_spec);
        m_fileChooserTarget.setSelection(m_settings.getTarget());

        // IAM service role
        final String iamServiceRoleArn = m_settings.getIamServiceRoleArn();
        boolean customServiceRole =
            ((DefaultComboBoxModel<String>)m_comboBoxRoleList.getModel()).getIndexOf(iamServiceRoleArn) < 0;
        m_buttonGroupRoleSelection.setSelected(m_radioButtonCustomRole.getModel(), customServiceRole);
        m_buttonGroupRoleSelection.setSelected(m_radioButtonAvailableRole.getModel(), !customServiceRole);
        if (customServiceRole) {
            m_textFieldCustomRole.setText(iamServiceRoleArn);
        } else {
            m_comboBoxRoleList.setSelectedItem(iamServiceRoleArn);
        }

        // Dataset group
        final String selectedDatasetGroup = m_settings.getSelectedDatasetGroup();
        boolean createNewDatasetGroup =
            ((DefaultComboBoxModel<String>)m_comboBoxDatasetGroupList.getModel()).getIndexOf(selectedDatasetGroup) < 0;
        m_buttonGroupDatasetGroup.setSelected(m_radioButtonUploadToNewDatasetGroup.getModel(), createNewDatasetGroup);
        m_buttonGroupDatasetGroup.setSelected(m_radioButtonUploadToExistingDatasetGroup.getModel(),
            !createNewDatasetGroup);
        if (createNewDatasetGroup) {
            m_textFieldDatasetGroupName.setText(selectedDatasetGroup);
        } else {
            m_comboBoxDatasetGroupList.setSelectedItem(selectedDatasetGroup);
        }
        final OverwritePolicy overwritePolicy = OverwritePolicy.valueOf(m_settings.getOverwriteDatasetPolicy());
        m_buttonGroupOverwriteDataset.setSelected(m_radioButtonOverwriteExistingDataset.getModel(),
            overwritePolicy == OverwritePolicy.OVERWRITE);
        m_buttonGroupOverwriteDataset.setSelected(m_radioButtonAbortOnExistingDataset.getModel(),
            overwritePolicy == OverwritePolicy.ABORT);

        m_textFieldDatasetName.setText(m_settings.getDatasetName());

        m_textFieldImportJobNamePrefix.setText(m_settings.getPrefixImportJobName());
        m_textFieldSchemaNamePrefix.setText(m_settings.getPrefixSchemaName());

        DataColumnSpecFilterConfiguration filterConfig = m_settings.getFilterConfig();
        m_columnFilterPanel.loadConfiguration(filterConfig, m_spec);

        enableComponents();
    }

    /**
     * Creates new settings object.
     *
     * @return settings
     */
    protected abstract S createSettings();

}
