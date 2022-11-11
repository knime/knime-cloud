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

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.sql.SQLException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.base.node.io.database.connection.util.DefaultDatabaseConnectionSettings;
import org.knime.cloud.aws.redshift.connector.utility.RedshiftDriverDetector;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;

/**
 * Model for the Amazon Redshift connector node.
 *
 * @author Ole Ostergaard, KNIME.com
 */
@Deprecated
class RedshiftConnectorNodeModel extends NodeModel {

    private final RedshiftConnectorSettings m_settings = new RedshiftConnectorSettings();

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (!RedshiftDriverDetector.getDriverName().equals(m_settings.getDriver())) {
            setWarningMessage(String.format("Using %s\ninstead of %s",
                RedshiftDriverDetector.mapToPrettyDriverName(RedshiftDriverDetector.getDriverName()),
                RedshiftDriverDetector.mapToPrettyDriverName(m_settings.getDriver())));
            m_settings.setDriver(RedshiftDriverDetector.getDriverName());
        }

        if ((m_settings.getCredentialName() == null) && ((m_settings.getUserName(getCredentialsProvider()) == null)
            || m_settings.getUserName(getCredentialsProvider()).isEmpty())) {
            throw new InvalidSettingsException("No credentials or username for authentication given");
        }
        if ((m_settings.getHost() == null) || m_settings.getHost().isEmpty()) {
            throw new InvalidSettingsException("No hostname for database server given");
        }

        return new PortObjectSpec[]{createSpec()};
    }

    private DatabaseConnectionPortObjectSpec createSpec() {
        String jdbcUrl = getJdbcUrl(m_settings);

        DatabaseConnectionSettings s = new DatabaseConnectionSettings(m_settings);
        s.setJDBCUrl(jdbcUrl);
        DatabaseConnectionPortObjectSpec spec = new DatabaseConnectionPortObjectSpec(s);
        return spec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        DatabaseConnectionPortObjectSpec spec = createSpec();

        try {
            spec.getConnectionSettings(getCredentialsProvider()).createConnection(getCredentialsProvider());
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidSettingsException
                | SQLException | IOException ex) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null) {
                cause = ex;
            }

            throw new SQLException("Could not create connection to database: " + cause.getMessage(), ex);
        }

        return new PortObject[]{new DatabaseConnectionPortObject(spec)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveConnection(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        DefaultDatabaseConnectionSettings s = new DefaultDatabaseConnectionSettings();
        s.validateConnection(settings, getCredentialsProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedConnection(settings, getCredentialsProvider());
    }

    /**
     * Returns the JDBC url depending on the present driver.
     *
     * @param settings The settings
     * @return JDBC URL based on the given settings
     */
    static String getJdbcUrl(final RedshiftConnectorSettings settings) {
        StringBuilder buf = new StringBuilder();
        if (RedshiftDriverDetector.rsDriverAvailable()) {
            buf.append(
                "jdbc:redshift://" + settings.getHost() + ":" + settings.getPort() + "/" + settings.getDatabaseName());
            if (settings.getUseSsl()) {
                buf.append("?ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory");
            }

            final String parameter = settings.getParameter();
            if (parameter != null && !parameter.trim().isEmpty()) {
                if (!settings.getUseSsl()) {
                    if (!parameter.startsWith("?")) {
                        buf.append("?");
                    }
                    if (parameter.startsWith("&")) {
                        buf.append(parameter.substring(1));
                    } else {
                        buf.append(parameter);
                    }
                } else {
                    if (parameter.startsWith("?")) {
                        buf.append("&");
                        buf.append(parameter.substring(1));
                    } else {
                        if (!parameter.startsWith("&")) {
                            buf.append("&");
                        }
                        buf.append(parameter);
                    }
                }
            }
        } else {
            buf.append("jdbc:postgresql://" + settings.getHost() + ":" + settings.getPort() + "/"
                + settings.getDatabaseName());
            if (settings.getUseSsl()) {
                buf.append("?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory");
            }
        }

        String jdbcUrl = buf.toString();
        NodeLogger.getLogger(RedshiftConnectorNodeModel.class).debug("Using jdbc url: " + jdbcUrl);
        return jdbcUrl;
    }

    RedshiftConnectorNodeModel() {
        super(new PortType[0], new PortType[]{DatabaseConnectionPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    }
}
