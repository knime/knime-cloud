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
 *   Created on Jan 7, 2017 by oole
 */
package org.knime.cloud.aws.redshift.connector.utility;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.database.connectors.postgresql.utility.PostgreSQLUtility;

/**
 * Detects registered Redshift Drivers, uses the PostgreSQL driver by if no Redshift driver is found.
 *
 * @author Ole Ostergaard, KNIME.com
 */
@Deprecated
public class RedshiftDriverDetector {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(RedshiftDriverDetector.class);

    /**
     * Searches for drivers and returns preferred driver to use (external drivers are preferred).
     *
     * @return the driver class name of the preferred driver to use.
     */
    public static String getDriverName() {
        String driverName;

        if (rsDriverAvailable()) {
            driverName = RedshiftUtility.DRIVER;
            LOGGER.debug("Using Proprietary Redshift Driver " + driverName);

        } else {
            driverName = PostgreSQLUtility.DRIVER;
            LOGGER.debug("Open-Source PostgreSQL Driver: " + driverName);
        }

        return driverName;
    }

    /**
     * Maps the driver name to a pretty string representation for dialogs.
     *
     * @param driverName the driver's name that should be mapped to it's pretty representation; must not be
     *            <code>null</code>
     * @return a pretty driver name for display purposes
     */
    public static String mapToPrettyDriverName(final String driverName) {
        if (driverName.equals(RedshiftUtility.DRIVER)) {
            return String.format("Proprietary Redshift Driver (%s)", driverName);
        } else if (driverName.equals(PostgreSQLUtility.DRIVER)) {
            return String.format("Open-Source PostgreSQL Driver (%s)", driverName);
        } else {
            return driverName;
        }
    }

    /**
     * Returns whether or not the proprietary Redshift driver has been registered.
     *
     * @return <code>true</code> if the Amazon driver has been registered, <code>false</code> otherwise
     */
    public static boolean rsDriverAvailable() {
        return DatabaseUtility.getJDBCDriverClasses().contains(RedshiftUtility.DRIVER);
    }
}
