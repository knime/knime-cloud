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
package org.knime.cloud.aws.redshift.connector2.loader;

import static java.util.Objects.requireNonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.knime.core.node.ExecutionMonitor;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.model.DBTable;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionReference;

/**
 * Redshift data loader.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBLoader implements DBLoader {

    private final DBSessionReference m_sessionReference;

    /**
     * Constructs a {@link RedshiftDBLoader} object.
     *
     * @param sessionReference the reference to the agent's session.
     */
    public RedshiftDBLoader(final DBSessionReference sessionReference) {
        m_sessionReference = requireNonNull(sessionReference, "sessionReference");
    }

    @Override
    public void load(final ExecutionMonitor exec, final Object parameters) throws Exception {
        @SuppressWarnings("unchecked")
        final DBLoadTableFromFileParameters<RedshiftLoaderSettings> loadParameters =
            (DBLoadTableFromFileParameters<RedshiftLoaderSettings>)parameters;
        final String filePath = loadParameters.getFilePath();
        final DBTable table = loadParameters.getTable();
        final RedshiftLoaderSettings additionalSettings = loadParameters.getAdditionalSettings()
            .orElseThrow(() -> new IllegalArgumentException("Missing additional settings."));
        final DBSession session = m_sessionReference.get();
        final DBSQLDialect dialect = session.getDialect();

        final StringBuilder buf = new StringBuilder();
        buf.append("copy ");
        buf.append(dialect.createFullName(table));
        buf.append(" from '");
        buf.append(filePath);
        buf.append("'\n");
        buf.append(additionalSettings.getAuthorization());
        buf.append("\n");
        RedshiftLoaderFileFormat fileFormat = additionalSettings.getFileFormat();
        buf.append(fileFormat.getFormatPart(additionalSettings));
        final String copyStmt = buf.toString();
        exec.checkCanceled();
        try (Connection connection = session.getConnectionProvider().getConnection(exec);
                Statement statement = connection.createStatement()) {
            exec.setMessage("Loading staged data into table (this might take some time without progress changes)");
                statement.execute(copyStmt);
            exec.setProgress(1);
        } catch (final Throwable throwable) {
            throw new SQLException(throwable.getMessage(), throwable);
        }
    }

}
