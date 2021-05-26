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
 *   2020-10-04 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.EnumMap;
import java.util.Map;

import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JPanel;
import javax.swing.JRadioButton;

import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.CustomerKeySource;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentFlowVariableNameSelection2;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.VariableType.CredentialsType;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.DialogComponentReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;

/**
 * Component for editing customer key settings for the SSE-C encryption mode. Allows entering the key directly,
 * selecting credential flow variable holding the key or read the key from a file.
 *
 * @author Alexander Bondaletov
 */
public class CustomerKeyInputPanel extends JPanel {
    private static final long serialVersionUID = 1L;

    private final S3ConnectorNodeSettings m_settings;
    private final S3ConnectorNodeDialog m_parent;

    private Map<CustomerKeySource, JRadioButton> m_radioButtons;

    private DialogComponentFlowVariableNameSelection2 m_flowVarSelector;

    private DialogComponentReaderFileChooser m_fileChooser;

    /**
     * @param settings The S3 Connector settings.
     * @param parent The parent dialog.
     */
    public CustomerKeyInputPanel(final S3ConnectorNodeSettings settings, final S3ConnectorNodeDialog parent) {
        m_settings = settings;
        m_parent = parent;

        initUI();
    }

    private void initUI() {
        m_radioButtons = new EnumMap<>(CustomerKeySource.class);
        ButtonGroup rbGroup = new ButtonGroup();
        JRadioButton rbEnterKey = createSourceRadioButton(CustomerKeySource.SETTINGS, rbGroup);
        JRadioButton rbSelectCreds = createSourceRadioButton(CustomerKeySource.CREDENTIAL_VAR, rbGroup);
        JRadioButton rbFile = createSourceRadioButton(CustomerKeySource.FILE, rbGroup);

        DialogComponentString keyInput = new DialogComponentString(m_settings.getCustomerKeyModel(), "", false, 40);
        m_flowVarSelector = new DialogComponentFlowVariableNameSelection2(m_settings.getCustomerKeyVarModel(), "",
            () -> m_parent.getAvailableFlowVariables(CredentialsType.INSTANCE));

        final SettingsModelReaderFileChooser fileModel = m_settings.getCustomerKeyFileModel();
        final FlowVariableModel fvm =
            m_parent.createFlowVariableModel(fileModel.getKeysForFSLocation(), FSLocationVariableType.INSTANCE);
        m_fileChooser = new DialogComponentReaderFileChooser(fileModel, "s3_sse_customer_key", fvm);

        setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.WEST;
        c.fill = GridBagConstraints.NONE;
        c.weightx = 0;
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = 0;
        c.insets = new Insets(5, 10, 5, 10);
        add(rbEnterKey, c);

        c.gridx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        add(keyInput.getComponentPanel().getComponent(1), c);

        c.fill = GridBagConstraints.NONE;
        c.gridy += 1;
        c.gridx = 0;
        add(rbSelectCreds, c);

        c.gridx = 1;
        add(m_flowVarSelector.getComponentPanel().getComponent(1), c);

        c.gridx = 0;
        c.gridy += 1;
        c.gridwidth = 2;
        add(rbFile, c);

        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        c.gridy += 1;
        add(m_fileChooser.getComponentPanel(), c);

        c.fill = GridBagConstraints.BOTH;
        c.gridy += 1;
        c.weighty = 1;
        add(Box.createVerticalGlue(), c);
    }

    private JRadioButton createSourceRadioButton(final CustomerKeySource source, final ButtonGroup group) {
        JRadioButton rb = new JRadioButton(source.getTitle());
        rb.addActionListener(e -> {
            m_settings.setCustomerKeySource(source);
            updateEnabledComponentss();
        });
        group.add(rb);
        m_radioButtons.put(source, rb);
        return rb;
    }

    private void updateEnabledComponentss() {
        boolean enabled = isEnabled();
        CustomerKeySource source = m_settings.getCustomerKeySource();

        m_settings.getCustomerKeyModel().setEnabled(enabled && source == CustomerKeySource.SETTINGS);
        m_settings.getCustomerKeyVarModel().setEnabled(enabled && source == CustomerKeySource.CREDENTIAL_VAR);
        m_settings.getCustomerKeyFileModel().setEnabled(enabled && source == CustomerKeySource.FILE);

        for (JRadioButton rb : m_radioButtons.values()) {
            rb.setEnabled(enabled);
        }
    }

    /**
     * Method intended to be called by parent dialog after settings are loaded.
     *
     * @param settings The settings.
     * @param specs The specs.
     * @throws NotConfigurableException
     */
    public void onSettingsLoaded(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_flowVarSelector.loadSettingsFrom(settings, specs);
        m_fileChooser.loadSettingsFrom(settings, specs);
        m_radioButtons.get(m_settings.getCustomerKeySource()).setSelected(true);
        updateEnabledComponentss();
    }

    @Override
    public void setEnabled(final boolean enabled) {
        super.setEnabled(enabled);
        updateEnabledComponentss();
    }
}
