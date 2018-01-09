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
 *   Apr 3, 2017 (oole): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.creator;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.cloud.aws.redshift.clustermanipulation.util.RedshiftClusterChooserComponent;
import org.knime.cloud.aws.redshift.clustermanipulation.util.RedshiftGeneralComponents;
import org.knime.cloud.aws.redshift.connector.utility.RedshiftUtility;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * The extended {@link RedshiftGeneralComponents} for the RedshiftClusterLauncherNodeModel.
 *
 * @author Ole Ostergaard, KNIME.com
 */
class RedshiftClusterLauncherComponents extends RedshiftGeneralComponents<RedshiftClusterLauncherNodeSettings> {

    private DialogComponentAuthentication m_clusterCredentials = defineClusterAuthenticationComponent();

    private DialogComponentString m_defaultDB =
        new DialogComponentString(m_settings.getDefaultDBModel(), "Database name:              ");

    private DialogComponentStringSelection m_nodeType =
        new DialogComponentStringSelection(m_settings.getNodeTypeModel(),
            "Node type:                                      ", RedshiftClusterLauncherNodeModel.getClusterTypes());

    private DialogComponentNumber m_nodeNumber = new DialogComponentNumber(m_settings.getNodeNumberModel(),
        "Number of nodes:                                          ", 1, 2);

    private DialogComponentNumber m_portNumber = new DialogComponentNumber(m_settings.getPortNumberModel(),
        "Port number:                                             ", RedshiftUtility.DEFAULT_PORT);

    private DialogComponentBoolean m_failIfExists =
        new DialogComponentBoolean(m_settings.getFailIfExistsModel(), "Fail if cluster exists");

    private RedshiftClusterChooserComponent<RedshiftClusterLauncherNodeSettings> m_clusterChoser;

    /**
     * Builds the dialog components for the RedshiftClusterLauncherDialog.
     *
     * @param settings The corresponding RedshiftClusterLauncherNodeSettings
     * @param cp The node's {@link CredentialsProvider}
     */
    public RedshiftClusterLauncherComponents(final RedshiftClusterLauncherNodeSettings settings,
        final CredentialsProvider cp) {
        super(settings);
        m_clusterChoser = new RedshiftClusterChooserComponent<RedshiftClusterLauncherNodeSettings>(
            m_settings.getClusterNameModel(), settings, settings.getPrefix(), cp);
    }

    /**
     * Get the {@link JPanel} for the Cloud connector dialog.
     *
     * @return The panel for the cloud connector dialog
     */
    @Override
    public JPanel getDialogPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        JPanel authPanel = super.getDialogPanel();
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(authPanel, gbc);
        gbc.gridy++;
        panel.add(createClusterComponent(), gbc);
        return panel;
    }

    /**
     * Create the panel for the cluster specific settings.
     *
     * @return The panel for the cluster specific settings
     */
    protected JPanel createClusterComponent() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(
            BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), " " + "Redshift Settings" + " "));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.weighty = 0;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.ABOVE_BASELINE_LEADING;
        //        panel.add(m_clusterName.getComponentPanel(), gbc);
        //        gbc.gridy++;
        panel.add(m_clusterChoser.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_portNumber.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_clusterCredentials.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_defaultDB.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_nodeType.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_nodeNumber.getComponentPanel(), gbc);
        gbc.gridy++;
        panel.add(m_failIfExists.getComponentPanel(), gbc);
        return panel;
    }

    /**
     * Create the authentication component for the Redshift master user, master password.
     *
     * @return The authentication component for the Redshift master user, master password
     */
    protected DialogComponentAuthentication defineClusterAuthenticationComponent() {
        final DialogComponentAuthentication authComponent =
            new DialogComponentAuthentication(m_settings.getClusterCredentials(), "Cluster credentials",
                AuthenticationType.USER_PWD, AuthenticationType.CREDENTIALS);
        authComponent.setUsernameLabel("Master user");
        authComponent.setPasswordLabel("Master password");
        return authComponent;
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        super.saveSettingsTo(settings);
        m_portNumber.saveSettingsTo(settings);
        m_clusterCredentials.saveSettingsTo(settings);
        m_defaultDB.saveSettingsTo(settings);
        m_nodeType.saveSettingsTo(settings);
        m_nodeNumber.saveSettingsTo(settings);
        m_failIfExists.saveSettingsTo(settings);
        m_clusterChoser.saveSettingsTo(settings);
    }

    /**
     * Loads the settings and passes the necessary credentials to the dialog to enable querying existing cluster names.
     *
     * @param settings the settings to load from
     * @param specs the specs to load from
     * @param cp The nodes {@link CredentialsProvider}
     * @param settingsModel The actual settings model
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final CredentialsProvider cp, final RedshiftClusterLauncherNodeSettings settingsModel)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs, cp);
        m_portNumber.loadSettingsFrom(settings, specs);
        m_clusterCredentials.loadSettingsFrom(settings, specs, cp);
        m_defaultDB.loadSettingsFrom(settings, specs);
        m_nodeType.loadSettingsFrom(settings, specs);
        m_nodeNumber.loadSettingsFrom(settings, specs);
        m_failIfExists.loadSettingsFrom(settings, specs);
        m_clusterChoser.loadSettingsFrom(settings, specs, cp, settingsModel);
    }
}
