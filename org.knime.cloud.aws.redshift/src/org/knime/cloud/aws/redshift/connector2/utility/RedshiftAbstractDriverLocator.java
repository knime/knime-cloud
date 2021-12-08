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

package org.knime.cloud.aws.redshift.connector2.utility;

import java.util.Collection;
import java.util.Set;

import org.knime.database.DBType;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.driver.AbstractDriverLocator;
import org.knime.database.driver.DBDriverLocator;

/**
 * Abstract class for shared code of different Amazon Redshift driver locators.
 *
 * @author Zkriya Rakhimberdiyev
 */
public abstract class RedshiftAbstractDriverLocator extends AbstractDriverLocator {

    /**
     * The {@link AttributeCollection} {@linkplain #getAttributes() of} Amazon Redshift drivers.
     */
    public static final AttributeCollection ATTRIBUTES;

    static {
        final AttributeCollection.Builder builder =
                AttributeCollection.builder(DBConnectionManagerAttributes.getAttributes());
        builder.add(Accessibility.EDITABLE, DBConnectionManagerAttributes.ATTRIBUTE_VALIDATION_QUERY, "SELECT 0");
        //change only visibility but keep the default values
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_TO_URL);
        builder.add(Accessibility.HIDDEN,
            DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_INITIAL_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_PARAMETER_SEPARATOR);
        builder.add(Accessibility.HIDDEN, DBConnectionManagerAttributes.ATTRIBUTE_APPEND_JDBC_USER_AND_PASSWORD_TO_URL);
        ATTRIBUTES = builder.build();
    }

    private final String m_version;

    private final Set<String> m_driverPaths;

    /**
     * Constructor for the driver locator.
     *
     * @param version the version of the driver
     * @param driverPaths the path to the driver jars
     */
    public RedshiftAbstractDriverLocator(final String version, final Set<String> driverPaths) {
        super(ATTRIBUTES);
        m_version = version;
        m_driverPaths = driverPaths;
    }

    /**
     * Returns a string representation of the driver version.
     *
     * @return the driver version
     */
    protected String getVersion() {
        return m_version;
    }

    @Override
    public String getDriverId() {
        return DBDriverLocator.createDriverId(getDBType(), getVersion());
    }

    @Override
    public String getDriverName() {
        return "Driver for Amazon Redshift v. " + getVersion();
    }

    @Override
    public DBType getDBType() {
        return Redshift.DB_TYPE;
    }

    @Override
    public Collection<String> getDriverPaths() {
        return m_driverPaths;
    }

    @Override
    public String getURLTemplate() {
        return "jdbc:redshift://<host>:<port>/[database]";
    }
}
