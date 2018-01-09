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
 *   Aug 11, 2016 (oole): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.util;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.util.Pair;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Holding the general components for the Amazon Redshift cluster manipulation nodes.
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 * @param <S> Extended {@link RedshiftGeneralSettings}
 */
public abstract class RedshiftGeneralComponents<S extends RedshiftGeneralSettings> {

    /**
     * The extendes {@link RedshiftGeneralSettings}
     */
    protected final S m_settings;

    private DialogComponentAuthentication m_auth;

    private final DialogComponentStringSelection m_region;

    private DialogComponentNumber m_pollingInterval;

    /**
     * Constructor.
     *
     * @param settings The settings for the dialog components
     */
    public RedshiftGeneralComponents(final S settings) {
        m_settings = settings;
        List<String> regions = loadRegions();
        m_region = new DialogComponentStringSelection(settings.getRegionModel(),
            "Region:                                      ", regions, false);
    }

    private ArrayList<String> loadRegions() {
        final S model = getSettings();
        final ArrayList<String> regionNames = new ArrayList<String>();
        for (final Regions regions : Regions.values()) {
            final Region region = Region.getRegion(regions);
            if (region.isServiceSupported(model.getPrefix())) {
                final String reg = region.getName();
                regionNames.add(reg);
            }
        }
        return regionNames;
    }

    /**
     * Get the {@link DialogComponentAuthentication} for the authentication.
     *
     * @return The dialog component for the authentication
     */
    protected DialogComponentAuthentication getAuthenticationComponent() {
        m_auth = defineAuthenticationComponent();
        return m_auth;
    }

    /**
     * Get the {@link DialogComponentNumber} for the timeout.
     *
     * @return The dialog component for the time out
     */
    protected DialogComponentNumber getPollingComponent() {
        m_pollingInterval = new DialogComponentNumber(m_settings.getPollingModel(),
            "Polling interval [s]:                                          ", 1);
        return m_pollingInterval;
    }

    /**
     * Get the {@link JPanel} for the Cloud connector dialog.
     *
     * @return The panel for the cloud connector dialog
     */
    public JPanel getDialogPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(
            BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), " " + "AWS Settings" + " "));

        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.weightx = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(getAuthenticationPanel(), gbc);
        gbc.gridy++;
        panel.add(getPollingComponent().getComponentPanel(), gbc);
        return panel;
    }

    /**
     * Loads the settings.
     *
     * @param settings The settings to load from
     * @param specs The specs to load from
     * @param cp The node {@link CredentialsProvider}
     * @throws NotConfigurableException If the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs,
        final CredentialsProvider cp) throws NotConfigurableException {
        m_auth.loadSettingsFrom(settings, specs, cp);
        m_pollingInterval.loadSettingsFrom(settings, specs);
        m_region.loadSettingsFrom(settings, specs);
    }

    /**
     * Saves the settings.
     *
     * @param settings the {@link NodeSettingsWO} to load from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_auth.saveSettingsTo(settings);
        m_pollingInterval.saveSettingsTo(settings);
        m_region.saveSettingsTo(settings);
    }

    /**
     * Get the settings for this components.
     *
     * @return The corresponding settings
     */
    public S getSettings() {
        return m_settings;
    }

    /**
     * Get the {@link JPanel} containing the {@link DialogComponentAuthentication}.
     *
     * @return The JPanel containing the {@link DialogComponentAuthentication}
     */
    protected JPanel getAuthenticationPanel() {
        final JPanel auth = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.BOTH;
        auth.add(getAuthenticationComponent().getComponentPanel());
        gbc.gridy++;
        gbc.fill = GridBagConstraints.NONE;
        auth.add(m_region.getComponentPanel(), gbc);
        return auth;
    }

    static HashMap<AuthenticationType, Pair<String, String>> getNameMap() {
        final HashMap<AuthenticationType, Pair<String, String>> nameMap = new HashMap<>();
        nameMap.put(AuthenticationType.USER_PWD, new Pair<String, String>("Access Key ID and Secret Key",
            "Access Key ID and Secret Access Key based authentication"));
        nameMap.put(AuthenticationType.KERBEROS, new Pair<String, String>("Default Credential Provider Chain",
            "Use the Default Credential Provider Chain for authentication"));
        return nameMap;
    }

    /**
     * This function should define the {@link DialogComponentAuthentication}. Including it's
     * {@link AuthenticationType}s.
     *
     * @return The defined {@link DialogComponentAuthentication} including it's {@link AuthenticationType}s and
     *         properties.
     */
    protected DialogComponentAuthentication defineAuthenticationComponent() {
        final HashMap<AuthenticationType, Pair<String, String>> nameMap = new HashMap<>();
        nameMap.put(AuthenticationType.USER_PWD, new Pair<String, String>("Access Key ID and Secret Key",
            "Access Key ID and Secret Access Key based authentication"));
        nameMap.put(AuthenticationType.KERBEROS, new Pair<String, String>("Default Credential Provider Chain",
            "Use the Default Credential Provider Chain for authentication"));
        final DialogComponentAuthentication authComponent =
            new DialogComponentAuthentication(m_settings.getAuthenticationModel(), "Authentication", nameMap,
                AuthenticationType.USER_PWD, AuthenticationType.CREDENTIALS, AuthenticationType.KERBEROS);
        authComponent.setUsernameLabel("Access Key ID");
        authComponent.setPasswordLabel("Secret Key");
        return authComponent;
    }
}
