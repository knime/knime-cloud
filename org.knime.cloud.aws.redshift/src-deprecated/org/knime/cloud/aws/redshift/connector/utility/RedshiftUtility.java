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
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jan 7, 2017 by oole
 */
package org.knime.cloud.aws.redshift.connector.utility;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.ObjectUtils.Null;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.function.AvgDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.BitAndDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.BitOrDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.CountDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MaxDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.MinDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.StdDevSampDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.SumDistinctDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarPopDBAggregationFunction;
import org.knime.core.node.port.database.aggregation.function.VarSampDBAggregationFunction;
import org.knime.core.node.port.database.connection.DefaultDBDriverFactory;
import org.knime.core.node.port.database.tablecreator.DBTableCreator;
import org.knime.core.node.port.database.tablecreator.DBTableCreatorIfNotExistsImpl;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

/**
 * Database utility for Amazon Redshift.
 *
 * @author Ole Ostergaard, KNIME.com
 */
@Deprecated
public class RedshiftUtility extends DatabaseUtility {

    private static class RedshiftStatementManipulator extends StatementManipulator {

        /**
         * Constructor of class {@link RedshiftStatementManipulator}.
         */
        public RedshiftStatementManipulator() {
            super(true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setFetchSize(final Statement statement, final int fetchSize) throws SQLException {
            if (RedshiftDriverDetector.rsDriverAvailable()) {
                super.setFetchSize(statement, fetchSize);
            } else {
                if (fetchSize >= 0) {
                    // fix 2741: postgresql databases ignore fetchsize when
                    // AUTOCOMMIT on; setting it to false
                    DatabaseConnectionSettings.setAutoCommit(statement.getConnection(), false);
                    super.setFetchSize(statement, fetchSize);
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String forMetadataOnly(final String sql) {
            return limitRows(sql, 0);
        }

        @Override
        public String randomRows(final String sql, final long count) {
            final String tmp = "SELECT * FROM (" + sql + ") " + getTempTableName() + " ORDER BY RANDOM() LIMIT " +count;
            return limitRows(tmp, count);
        }

    }




    private static final StatementManipulator MANIPULATOR = new RedshiftStatementManipulator();

    /** The unique database identifier. */
    public static final String DATABASE_IDENTIFIER = "redshift";

    /** The driver's class name. */
    public static final String DRIVER = RedshiftDriverFactory.RS_DRIVER_NAME;

    /** The Redshift default port number */
    public static final Integer DEFAULT_PORT = 5439;

    /**
     * Constructor.
     *
     * Uses the {@link RedshiftDriverFactory} when it's available otherwise it falls back to the
     * PostgreSQL driver.
     */
    public RedshiftUtility() {
        super(DATABASE_IDENTIFIER, MANIPULATOR, getFactory(),
            new AvgDistinctDBAggregationFunction.Factory(), new BitAndDBAggregationFunction.Factory(),
            new BitOrDBAggregationFunction.Factory(), new CountDistinctDBAggregationFunction.Factory(),
            new MaxDBAggregationFunction.Factory(), new MinDBAggregationFunction.Factory(),
            new SumDistinctDBAggregationFunction.Factory(), new StdDevPopDBAggregationFunction.Factory(),
            new StdDevSampDBAggregationFunction.Factory(), new VarPopDBAggregationFunction.Factory(),
            new VarSampDBAggregationFunction.Factory());
    }

    /**
     * Returns the {@link RedshiftDriverFactory} when it's driver is available and {@link Null} otherwise.
     *
     * @return The {@link DefaultDBDriverFactory} for the redshift driver or {@link Null} when it's not available
     */
    private static DefaultDBDriverFactory getFactory() {
        DefaultDBDriverFactory driverFactory = null;
        Bundle bundle = FrameworkUtil.getBundle(RedshiftUtility.class);
        try {
            bundle.loadClass(DRIVER);
            driverFactory = new RedshiftDriverFactory(DRIVER, bundle);
        } catch (ClassNotFoundException | IOException ex) {
            // driver fragment not installed
        }
        return driverFactory;
    }

    @Override
    public boolean supportsRandomSampling() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsCase() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBTableCreator getTableCreator(final String schema, final String tableName, final boolean isTempTable) {
        return new DBTableCreatorIfNotExistsImpl(getStatementManipulator(), schema, tableName, isTempTable);
    }
}
