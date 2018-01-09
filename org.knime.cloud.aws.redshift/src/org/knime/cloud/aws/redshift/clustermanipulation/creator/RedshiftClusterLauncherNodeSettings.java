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
 *   Mar 24, 2017 (oole): created
 */
package org.knime.cloud.aws.redshift.clustermanipulation.creator;

import org.knime.cloud.aws.redshift.clustermanipulation.util.RedshiftGeneralSettings;
import org.knime.cloud.aws.redshift.connector.utility.RedshiftUtility;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * The extended {@link RedshiftGeneralSettings} for the RedshiftClusterLauncherNodeModel.
 *
 * @author Ole Ostergaard, KNIME.com
 */
class RedshiftClusterLauncherNodeSettings extends RedshiftGeneralSettings {

    /**
     * Constructor.
     *
     * @param prefix The connection's prefix
     */
    public RedshiftClusterLauncherNodeSettings(final String prefix) {
        super(prefix);
    }

    /** Holds the clusters name */
    protected final SettingsModelString m_clusterName = new SettingsModelString("clusterName", "");

    /** Holds the default DB name */
    protected final SettingsModelString m_defaultDB = new SettingsModelString("defaultBD", "");

    /** Holds the master name for the cluster */
    protected final SettingsModelAuthentication m_clusterCredentials =
        new SettingsModelAuthentication("clusterLogin", SettingsModelAuthentication.AuthenticationType.USER_PWD);

    /** The node type */
    protected final SettingsModelString m_nodeType = new SettingsModelString("nodeType", "");

    /** The number of nodes */
    protected final SettingsModelInteger m_nodeNumber = new SettingsModelIntegerBounded("nodes", 1, 1, 32);

    /** The port number for the database */
    protected final SettingsModelInteger m_portNumber = new SettingsModelInteger("port", RedshiftUtility.DEFAULT_PORT);

    /** Whether the node should fail if the cluster already exists */
    protected final SettingsModelBoolean m_failIfExists = new SettingsModelBoolean("failIfExists", false);

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_clusterName.saveSettingsTo(settings);
        m_defaultDB.saveSettingsTo(settings);
        m_clusterCredentials.saveSettingsTo(settings);
        m_nodeType.saveSettingsTo(settings);
        m_nodeNumber.saveSettingsTo(settings);
        m_portNumber.saveSettingsTo(settings);
        m_failIfExists.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadValidatedSettings(settings);
        m_clusterName.loadSettingsFrom(settings);
        m_defaultDB.loadSettingsFrom(settings);
        m_clusterCredentials.loadSettingsFrom(settings);
        m_nodeType.loadSettingsFrom(settings);
        m_nodeNumber.loadSettingsFrom(settings);
        m_portNumber.loadSettingsFrom(settings);
        m_failIfExists.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_clusterName.validateSettings(settings);
        m_defaultDB.validateSettings(settings);
        m_clusterCredentials.validateSettings(settings);
        m_nodeType.validateSettings(settings);
        m_nodeNumber.validateSettings(settings);
        m_portNumber.validateSettings(settings);
        m_failIfExists.validateSettings(settings);
    }

    /**
     * Returns the cluster name.
     *
     * @return The cluster name
     */
    public String getClusterName() {
        return m_clusterName.getStringValue();
    }

    /**
     * Return the default database name.
     *
     * @return The default database name
     */
    public String getDefaultDBName() {
        return m_defaultDB.getStringValue();
    }

    /**
     * Returns the master user name for the cluster.
     *
     * @return The master name for the cluster
     */
    public String getMasterName() {
        return m_clusterCredentials.getUsername();
    }

    /**
     * Returns the master password for the cluster.
     *
     * @return The master password for the cluster
     */
    public String getMasterPassword() {
        return m_clusterCredentials.getPassword();
    }

    /**
     * Returns the node type.
     *
     * @return The node type
     */
    public String getNodeType() {
        return m_nodeType.getStringValue();
    }

    /**
     * Returns the port for the database.
     *
     * @return The port for the database
     */
    public Integer getPort() {
        return m_portNumber.getIntValue();
    }

    /**
     * Returns the number of nodes.
     *
     * @return The number of nodes
     */
    public Integer getNodeNumber() {
        return m_nodeNumber.getIntValue();
    }

    /**
     * Returns whether the node should fail if the cluster already exists.
     *
     * @return Whether the node should fail if the cluster already exists
     */
    public Boolean failIfExists() {
        return m_failIfExists.getBooleanValue();
    }

    /**
     * Returns the model for the cluster name.
     *
     * @return The model for the cluster name
     */
    public SettingsModelString getClusterNameModel() {
        return m_clusterName;
    }

    /**
     * Returns the model for the default database name.
     *
     * @return The model for the default database name
     */
    public SettingsModelString getDefaultDBModel() {
        return m_defaultDB;
    }

    /**
     * Return the model for the cluster credentials.
     *
     * @return The model for the given cluster credentials
     */
    public SettingsModelAuthentication getClusterCredentials() {
        return m_clusterCredentials;
    }

    /**
     * Returns the model for the node type.
     *
     * @return The model for the node type
     */
    public SettingsModelString getNodeTypeModel() {
        return m_nodeType;
    }

    /**
     * Returns the model for the number of nodes.
     *
     * @return The model for the number of nodes
     */
    public SettingsModelInteger getNodeNumberModel() {
        return m_nodeNumber;
    }

    /**
     * Returns the model for the port number.
     *
     * @return The model for the port number
     */
    public SettingsModelInteger getPortNumberModel() {
        return m_portNumber;
    }

    /**
     * Returns the model for whether the node should fail if the cluster already exists.
     *
     * @return The model for whether the node should fail if the cluster already exists
     */
    public SettingsModelBoolean getFailIfExistsModel() {
        return m_failIfExists;
    }
}
