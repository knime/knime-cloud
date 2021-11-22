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
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.Arrays;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.cloud.aws.filehandling.s3.fs.S3FSConnection;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistoryPanel;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.auth.AuthPanel;
import org.knime.filehandling.core.connections.base.auth.AuthSettings;
import org.knime.filehandling.core.connections.base.auth.EmptyAuthProviderPanel;
import org.knime.filehandling.core.connections.base.auth.StandardAuthTypes;

/**
 * Generic S3 connector node dialog that combines Amazon Authentication and S3 Connector Dialog with a custom endpoint URL.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class S3GenericConnectorNodeDialog extends S3ConnectorNodeDialog {
    private StringHistoryPanel m_endpointUrlPanel;

    private AuthPanel m_authPanel;

    private StringHistoryPanel m_regionPanel;

    S3GenericConnectorNodeDialog(final PortsConfiguration portsConfig) {
        super(new S3GenericConnectorNodeSettings(portsConfig));
    }

    private S3GenericConnectorNodeSettings getCompSettings() {
        return (S3GenericConnectorNodeSettings)m_settings;
    }

    @Override
    protected JComponent createSettingsTab() {
        final var panel = new JPanel(new GridBagLayout());

        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(createSettingsEndpointPanel(), gbc);

        gbc.gridy++;
        panel.add(createAuthPanel(), gbc);

        gbc.gridy++;
        panel.add(createFileSystemPanel(), gbc);

        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(Box.createVerticalGlue(), gbc);

        return panel;
    }

    @Override
    protected JComponent createAdvancedTab() {
        final var panel = new JPanel(new GridBagLayout());

        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(createAdvancedEndpointPanel(), gbc);

        final var connectionTimeout = new DialogComponentNumber(
            getCompSettings().getConnectionTimeoutModel(), "Connect timeout in seconds: ", 10, 5);

        gbc.gridy++;
        panel.add(createTimeoutsPanel(connectionTimeout.getComponentPanel()), gbc);

        gbc.gridy++;
        panel.add(createEncryptionPanel(), gbc);

        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(Box.createVerticalGlue(), gbc);

        return panel;
    }

    private JComponent createSettingsEndpointPanel() {
        m_endpointUrlPanel = new StringHistoryPanel("s3-compatible-endpoint");

        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Endpoint"));

        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.insets = new Insets(0, 10, 0, 0);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new JLabel("URL:"), gbc);

        gbc.gridx++;
        gbc.insets = new Insets(5, 5, 5, 15);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(m_endpointUrlPanel, gbc);

        return panel;
    }

    private JComponent createAuthPanel() {
        final var authSettings = getCompSettings().getAuthSettings();
        m_authPanel = new AuthPanel(authSettings, //
            Arrays.asList( //
                new EmptyAuthProviderPanel( //
                    authSettings.getSettingsForAuthType(StandardAuthTypes.ANONYMOUS)), //
                new AccessSecretKeyAuthProviderPanel( //
                    authSettings.getSettingsForAuthType(S3GenericConnectorNodeSettings.ACCESS_KEY_AND_SECRET_AUTH), //
                    this), //
                new EmptyAuthProviderPanel( //
                    authSettings.getSettingsForAuthType(S3GenericConnectorNodeSettings.DEFAULT_PROVIDER_CHAIN_AUTH)) //
            ));
        m_authPanel.setBorder(BorderFactory.createTitledBorder("Authentication"));
        return m_authPanel;
    }

    private JComponent createAdvancedEndpointPanel() {
        m_regionPanel = new StringHistoryPanel("s3-compatible-region");

        final var panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Endpoint"));

        final var gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(0, 5, 0, 15);
        final var pathStyle = new DialogComponentBoolean( //
            getCompSettings().getPathStyleModel(), "Use path-style requests");
        panel.add(pathStyle.getComponentPanel(), gbc);

        gbc.gridx++;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(Box.createHorizontalGlue(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 10, 0, 0);
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new JLabel("Region:"), gbc);

        gbc.gridx++;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(5, 5, 5, 15);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(m_regionPanel, gbc);

        return panel;
    }

    @Override
    protected FSConnection createFSConnection() throws IOException {
        try {
            getCompSettings().getEndpointURLModel().setStringValue(m_endpointUrlPanel.getSelectedString());
            getCompSettings().getRegionModel().setStringValue(m_regionPanel.getSelectedString());
            return new S3FSConnection(getCompSettings().toFSConnectionConfig(getCredentialsProvider()));
        } catch (InvalidSettingsException ex) {
            throw new IOException("Unable to create connection configuration: " + ex.getMessage(), ex);
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        getCompSettings().getEndpointURLModel().setStringValue(m_endpointUrlPanel.getSelectedString());
        m_authPanel.saveSettingsTo(settings.addNodeSettings(AuthSettings.KEY_AUTH));
        getCompSettings().getRegionModel().setStringValue(m_regionPanel.getSelectedString());
        super.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        super.loadSettingsFrom(settings, specs);
        m_workingDirChooser.setEnableBrowsing(true);

        final var endpoint = getCompSettings().getEndpointURLModel().getStringValue();
        m_endpointUrlPanel.setSelectedString(endpoint);
        if (!endpoint.isBlank()) {
            m_endpointUrlPanel.commitSelectedToHistory();
            m_endpointUrlPanel.updateHistory();
        }

        try {
            m_authPanel.loadSettingsFrom(settings.getNodeSettings(AuthSettings.KEY_AUTH), specs);
        } catch (InvalidSettingsException e) { // NOSONAR ignore and use defaults
        }

        final var region = getCompSettings().getRegionModel().getStringValue();
        m_regionPanel.setSelectedString(region);
        if (!region.isBlank()) {
            m_regionPanel.commitSelectedToHistory();
            m_regionPanel.updateHistory();
        }
    }

}
