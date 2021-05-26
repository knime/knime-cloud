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
 *   Oct 26, 2019 (Tobias): created
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.awt.CardLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.event.ChangeListener;

import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.SSEMode;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.ui.WorkingDirectoryChooser;

/**
 * S3 connector node dialog.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class S3ConnectorNodeDialog extends NodeDialogPane {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(S3ConnectorNodeDialog.class);

    private final ChangeListener m_workdirListener;
    private final ChangeListener m_sseKmsUseAwsManagedListener;

    private final S3ConnectorNodeSettings m_settings;
    private final WorkingDirectoryChooser m_workingDirChooser =
        new WorkingDirectoryChooser("s3.workingDir", this::createFSConnection);

    private CloudConnectionInformation m_connInfo;
    private JComboBox<SSEMode> m_sseModeCombobox;
    private KmsKeyInputPanel m_kmsKeyInput;
    private CustomerKeyInputPanel m_customerKeyInput;

    S3ConnectorNodeDialog(final PortsConfiguration portsConfig) {
        m_settings = new S3ConnectorNodeSettings(portsConfig);
        m_workdirListener = e -> m_settings.getWorkingDirectoryModel()
            .setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());
        m_sseKmsUseAwsManagedListener = e -> onSseEnabledChanged();

        addTab("Settings", createSettingsPanel());
        addTab("Advanced", createAdvancedTab());
    }

    private JComponent createSettingsPanel() {
        DialogComponentBoolean normalizePath =
            new DialogComponentBoolean(m_settings.getNormalizePathModel(), "Normalize Paths");
        normalizePath.getComponentPanel().setLayout(new FlowLayout(FlowLayout.LEFT));

        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.weightx = 1;
        c.weighty = 0;
        c.gridx = 0;
        c.gridy = 0;
        c.insets = new Insets(0, 10, 0, 0);
        panel.add(m_workingDirChooser, c);

        c.gridy +=1;
        c.insets = new Insets(0, 0, 0, 0);
        panel.add(normalizePath.getComponentPanel(), c);

        c.fill = GridBagConstraints.BOTH;
        c.weighty = 1;
        c.gridy +=1;
        panel.add(Box.createVerticalGlue(), c);

        panel.setBorder(BorderFactory.createTitledBorder("File system settings"));
        return panel;
    }

    private JComponent createAdvancedTab() {
        JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(createTimeoutsPanel(), gbc);

        gbc.gridy++;
        panel.add(createEncryptionPanel(), gbc);

        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(Box.createVerticalGlue(), gbc);

        return panel;
    }

    private JComponent createTimeoutsPanel() {
        DialogComponentNumber socketTimeout =
                new DialogComponentNumber(m_settings.getSocketTimeoutModel(), "Read/write timeout in seconds: ", 10, 5);

        socketTimeout.getComponentPanel().setLayout(new FlowLayout(FlowLayout.LEFT));
        socketTimeout.getComponentPanel().setBorder(BorderFactory.createTitledBorder("Connection settings"));

        return socketTimeout.getComponentPanel();
    }

    private JComponent createEncryptionPanel() {
        m_kmsKeyInput = new KmsKeyInputPanel(m_settings.getKmsKeyIdModel());
        m_customerKeyInput = new CustomerKeyInputPanel(m_settings, this);

        JPanel cards = new JPanel(new CardLayout());
        cards.add(new JPanel(), SSEMode.S3.getKey());
        cards.add(createSseKmsPanel(), SSEMode.KMS.getKey());
        cards.add(m_customerKeyInput, SSEMode.CUSTOMER_PROVIDED.getKey());

        DialogComponentBoolean sseEnabled =
            new DialogComponentBoolean(m_settings.getSseEnabledModel(), "Server-side encryption (SSE)");
        m_settings.getSseEnabledModel().addChangeListener(e -> onSseEnabledChanged());

        m_sseModeCombobox = new JComboBox<>(SSEMode.values());
        m_sseModeCombobox.addActionListener(e -> {
            SSEMode mode = (SSEMode)m_sseModeCombobox.getSelectedItem();
            m_settings.getSseModeModel().setStringValue(mode.getKey());
            ((CardLayout)cards.getLayout()).show(cards, mode.getKey());
        });


        JPanel checkboxPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
        checkboxPanel.add(sseEnabled.getComponentPanel());
        checkboxPanel.add(m_sseModeCombobox);
        checkboxPanel.add(Box.createHorizontalGlue());

        JPanel encryptionPanel = new JPanel(new GridBagLayout());
        encryptionPanel.setBorder(BorderFactory.createTitledBorder("Server side encryption"));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 5);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.NONE;
        encryptionPanel.add(checkboxPanel, gbc);

        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        encryptionPanel.add(Box.createHorizontalGlue(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 10, 0, 5);
        gbc.gridwidth = 2;
        gbc.weightx = 1;
        encryptionPanel.add(cards, gbc);

        return encryptionPanel;
    }

    private JPanel createSseKmsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 15, 0, 5);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new DialogComponentBoolean(m_settings.getSseKmsUseAwsManagedModel(),
                "Use default AWS managed key").getComponentPanel(), gbc);

        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(0, 25, 0, 5);
        gbc.gridwidth = 2;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(m_kmsKeyInput, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.weighty = 1;
        panel.add(Box.createVerticalGlue(), gbc);

        return panel;
    }

    private void onSseEnabledChanged() {
        boolean enabled = m_settings.isSseEnabled();
        m_sseModeCombobox.setEnabled(enabled);
        m_kmsKeyInput.setEnabled(enabled && !m_settings.sseKmsUseAwsManaged());
        m_customerKeyInput.setEnabled(enabled);
    }

    private FSConnection createFSConnection() {
        S3ConnectorNodeSettings clonedSettings = m_settings.createClone();
        return new S3FSConnection(m_connInfo, clonedSettings, getCredentialsProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.saveSettingsTo(settings);
        m_workingDirChooser.addCurrentSelectionToHistory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException ex) {
            LOGGER.warn(ex.getMessage(), ex);
        }

        m_connInfo = ((CloudConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();

        m_customerKeyInput.onSettingsLoaded(settings, specs);
        settingsLoaded();
    }

    private void settingsLoaded() {
        m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectoryModel().getStringValue());
        m_kmsKeyInput.onSettingsLoaded(m_connInfo);
        m_sseModeCombobox.setSelectedItem(m_settings.getSseMode());
    }

    @Override
    public void onOpen() {
        m_workingDirChooser.addListener(m_workdirListener);
        m_settings.getSseKmsUseAwsManagedModel().addChangeListener(m_sseKmsUseAwsManagedListener);
        onSseEnabledChanged();
    }

    @Override
    public void onClose() {
        m_workingDirChooser.removeListener(m_workdirListener);
        m_settings.getSseKmsUseAwsManagedModel().removeChangeListener(m_sseKmsUseAwsManagedListener);
        m_workingDirChooser.onClose();
    }
}
