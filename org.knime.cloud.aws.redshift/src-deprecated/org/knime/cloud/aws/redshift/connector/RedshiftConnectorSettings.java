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
 *
 * History
 *   May 12, 2014 ("Patrick Winter"): created
 */
package org.knime.cloud.aws.redshift.connector;

import org.knime.base.node.io.database.connection.util.DefaultDatabaseConnectionSettings;
import org.knime.cloud.aws.redshift.connector.utility.RedshiftUtility;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * Model for the Amazon Redshift connector node.
 *
 * @author Ole Ostergaard, KNIME.com
 */
@Deprecated
class RedshiftConnectorSettings extends DefaultDatabaseConnectionSettings {

    private static final String CFG_KEY = "redshift-connection";

    private static final String CFG_SSL = "useSsl";

    private boolean m_useSsl = true;

    private static final String CFG_PARAMETER = "parameter";

    private String m_parameter = "";

    RedshiftConnectorSettings() {
        setPort(RedshiftUtility.DEFAULT_PORT);
        setRetrieveMetadataInConfigure(true);
        setDatabaseIdentifier(RedshiftUtility.DATABASE_IDENTIFIER);
    }

    /**
     * Returns whether ssl should be used or not.
     *
     * @return whether ssl should be used or not
     */
    public boolean getUseSsl() {
        return m_useSsl;
    }

    /**
     * Returns the additional parameters or an empty string.
     *
     * @return the additional parameter or an empty string
     */
    public String getParameter() {
        return m_parameter;
    }

    /**
     * Sets the additional parameter in the SettingsModel.
     *
     * @param parameter the additional parameter to be set
     */
    public void setParamter(final String parameter) {
        m_parameter = parameter;
    }

    /**
     * Sets wether ssl is to be used or not.
     *
     * @param useSsl wether ssl should be used or not
     */
    public void setUseSsl(final boolean useSsl) {
        m_useSsl = useSsl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveConnection(final ConfigWO settings) {
        super.saveConnection(settings);
        Config config = settings.addConfig("redshift-connection");
        config.addBoolean("useSsl", m_useSsl);
        config.addString(CFG_PARAMETER, m_parameter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateConnection(final ConfigRO settings, final CredentialsProvider cp)
        throws InvalidSettingsException {
        super.validateConnection(settings, cp);
        Config config = settings.getConfig(CFG_KEY);
        config.getBoolean(CFG_SSL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean loadValidatedConnection(final ConfigRO settings, final CredentialsProvider cp)
        throws InvalidSettingsException {
        boolean b = super.loadValidatedConnection(settings, cp);
        Config config = settings.getConfig(CFG_KEY);
        m_useSsl = config.getBoolean(CFG_SSL);
        m_parameter = config.getString(CFG_PARAMETER);
        setRowIdsStartWithZero(true);
        return b;
    }

}
