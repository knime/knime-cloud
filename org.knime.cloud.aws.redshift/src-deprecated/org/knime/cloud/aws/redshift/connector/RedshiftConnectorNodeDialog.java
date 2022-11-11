/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.node.io.database.connection.util.DBAuthenticationPanel;
import org.knime.base.node.io.database.connection.util.DBConnectionPanel;
import org.knime.base.node.io.database.connection.util.DBMiscPanel;
import org.knime.base.node.io.database.connection.util.DBTimezonePanel;
import org.knime.cloud.aws.redshift.connector.utility.RedshiftDriverDetector;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.util.StringHistoryPanel;
import org.knime.database.connectors.commons.DatabaseIcons;

/**
 * Dialog for the Amazon Redshift Connector node.
 *
 * @author Ole Ostergaard, KNIME.com
 */
@Deprecated
class RedshiftConnectorNodeDialog extends NodeDialogPane {

    private class RedshiftConnectionPanel extends DBConnectionPanel<RedshiftConnectorSettings> {
        private static final long serialVersionUID = -9114605968206792444L;

        private final JCheckBox m_useSsl = new JCheckBox("SSL");

        RedshiftConnectionPanel(final RedshiftConnectorSettings settings) {
            super(settings, RedshiftConnectorNodeDialog.class.getName());

            m_c.gridx = 0;
            m_c.gridy++;
            add(new JLabel("Parameter "), m_c);
            m_c.gridy++;
            m_c.fill = GridBagConstraints.HORIZONTAL;
            m_c.weightx = 1;
            add(m_parameter, m_c);

            m_c.gridy++;
            add(m_useSsl, m_c);

            m_c.gridy++;
            add(new JLabel("Driver "), m_c);

            m_c.gridy++;
            m_c.fill = GridBagConstraints.HORIZONTAL;
            m_c.weightx = 1;
            add(m_driver, m_c);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void loadSettings(final PortObjectSpec[] specs) throws NotConfigurableException {
            super.loadSettings(specs);
            m_useSsl.setSelected(m_settings.getUseSsl());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void saveSettings() throws InvalidSettingsException {
            super.saveSettings();
            m_settings.setUseSsl(m_useSsl.isSelected());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected String getJDBCURL(final String host, final int port, final String dbName) {
            return RedshiftConnectorNodeModel.getJdbcUrl(m_settings);
        }
    }

    private final JLabel m_driver = new JLabel();

    private final RedshiftConnectorSettings m_settings = new RedshiftConnectorSettings();

    private final StringHistoryPanel m_parameter = new StringHistoryPanel(getClass().getName() + "_parameter    ");

    private final RedshiftConnectionPanel m_connectionPanel = new RedshiftConnectionPanel(m_settings);

    private final DBAuthenticationPanel<DatabaseConnectionSettings> m_authPanel =
        new DBAuthenticationPanel<DatabaseConnectionSettings>(m_settings);

    private final DBTimezonePanel<DatabaseConnectionSettings> m_tzPanel =
        new DBTimezonePanel<DatabaseConnectionSettings>(m_settings);

    private final DBMiscPanel<DatabaseConnectionSettings> m_miscPanel =
        new DBMiscPanel<DatabaseConnectionSettings>(m_settings, true);

    RedshiftConnectorNodeDialog() {
        JPanel p = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();
        c.gridy = 0;
        c.weightx = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(0, 0, 4, 0);

        p.add(m_connectionPanel, c);
        c.gridy++;
        p.add(m_authPanel, c);
        c.gridy++;
        p.add(m_tzPanel, c);
        c.gridy++;
        c.insets = new Insets(0, 0, 0, 0);
        p.add(m_miscPanel, c);

        addTab("Connection settings", p);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        try {
            m_settings.loadValidatedConnection(settings, getCredentialsProvider());
        } catch (InvalidSettingsException ex) {
            // too bad, use default values
        }

        m_connectionPanel.loadSettings(specs);
        m_authPanel.loadSettings(specs, getCredentialsProvider());
        m_tzPanel.loadSettings(specs);
        m_miscPanel.loadSettings(specs);
        m_parameter.setSelectedString(m_settings.getParameter());
        m_parameter.commitSelectedToHistory();
        m_parameter.updateHistory();

        if (!RedshiftDriverDetector.getDriverName().equals(m_settings.getDriver())) {
            m_driver.setIcon(DatabaseIcons.WARNING_ICON);
            m_driver.setToolTipText(String.format(
                "Clicking 'OK' or 'Apply' will change the driver to the one displayed and the node will be reset.",
                m_settings.getDriver()));
            m_driver.setText(RedshiftDriverDetector.mapToPrettyDriverName(RedshiftDriverDetector.getDriverName()));
        } else {
            m_driver.setIcon(null);
            m_driver.setToolTipText(null);
            m_driver.setText(RedshiftDriverDetector.mapToPrettyDriverName(m_settings.getDriver()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_connectionPanel.saveSettings();
        m_settings.setJDBCUrl(RedshiftConnectorNodeModel.getJdbcUrl(m_settings));
        m_authPanel.saveSettings();
        m_tzPanel.saveSettings();
        m_miscPanel.saveSettings(getCredentialsProvider());
        m_settings.setDriver(RedshiftDriverDetector.getDriverName());

        m_settings.setParamter(m_parameter.getSelectedString());
        m_settings.saveConnection(settings);
    }
}
