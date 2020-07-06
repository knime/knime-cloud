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

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.event.ChangeListener;

import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
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

    private final ChangeListener m_workdirListener;
    private final S3ConnectorNodeSettings m_settings = new S3ConnectorNodeSettings();
    private final WorkingDirectoryChooser m_workingDirChooser =
        new WorkingDirectoryChooser("s3.workingDir", this::createFSConnection);

    private CloudConnectionInformation m_connInfo;

    S3ConnectorNodeDialog() {
        m_workdirListener = e -> m_settings.getWorkingDirectoryModel()
            .setStringValue(m_workingDirChooser.getSelectedWorkingDirectory());

        addTab("Settings", createSettingsPanel());
        addTab("Advanced", createTimeoutsPanel());
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

    private JComponent createTimeoutsPanel() {
        DialogComponentNumber socketTimeout =
                new DialogComponentNumber(m_settings.getSocketTimeoutModel(), "Read/write timeout in seconds: ", 10, 5);

        socketTimeout.getComponentPanel().setLayout(new FlowLayout(FlowLayout.LEFT));
        socketTimeout.getComponentPanel().setBorder(BorderFactory.createTitledBorder("Connection settings"));

        return socketTimeout.getComponentPanel();
    }

    private FSConnection createFSConnection() throws IOException {
        S3ConnectorNodeSettings clonedSettings = m_settings.clone();
        return new S3FSConnection(m_connInfo, clonedSettings);
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
            // ignore
        }

        m_connInfo = ((CloudConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();

        settingsLoaded();
    }

    private void settingsLoaded() {
        m_workingDirChooser.setSelectedWorkingDirectory(m_settings.getWorkingDirectoryModel().getStringValue());
        m_workingDirChooser.addListener(m_workdirListener);
    }

    @Override
    public void onClose() {
        m_workingDirChooser.removeListener(m_workdirListener);
        m_workingDirChooser.onClose();
    }
}
