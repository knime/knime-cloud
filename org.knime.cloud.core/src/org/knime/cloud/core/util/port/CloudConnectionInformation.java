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
 *   Aug 30, 2016 (oole): created
 */
package org.knime.cloud.core.util.port;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;

/**
 * Extended {@link ConnectionInformation}. This provides functionality to have information about whether or not to use a
 * credentials key chain. This is just a flag. To use of a key chain must be implemented in the cloud storage connection
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class CloudConnectionInformation extends ConnectionInformation {

    private static final long serialVersionUID = 1L;

    private boolean m_useKeyChain;

    private static final String KEY_CHAIN_KEY = "keyChain";

    private boolean m_useSSEncryption;

    private static final String SSE_KEY = "ssencryption";

    private boolean m_switchRole;

    private String m_switchRoleAccount;

    private String m_switchRoleName;

    private static final String SWITCH_ROLE_KEY = "switchRole";

    private static final String SWITCH_ROLE_ACCOUNT_KEY = "switchRoleAccount";

    private static final String SWITCH_ROLE_NAME_KEY = "switchRoleName";

    private boolean m_useAnonymous = false;

    private static final String USE_ANONYMOUS_KEY = "useAnonymous";

    /**
     * Optional human readable service name (e.g. Amazon S3).
     */
    private String m_serviceName = "";

    private static final String CFG_SERVICE_NAME = "serviceName";

    /**
     * Parameterless constructor
     */
    public CloudConnectionInformation() {
    }

    /**
     * @param model
     * @throws InvalidSettingsException
     */
    protected CloudConnectionInformation(final ModelContentRO model) throws InvalidSettingsException {
        super(model);
        this.setUseKeyChain(model.getBoolean(KEY_CHAIN_KEY, false));
        // New Server Side Encryption AP-8823
        if (model.containsKey(SSE_KEY)) {
            this.setUseSSEncryption(model.getBoolean(SSE_KEY));
        } else {
            this.setUseSSEncryption(false);
        }
        if (model.containsKey(SWITCH_ROLE_KEY)) {
            this.setSwitchRole(model.getBoolean(SWITCH_ROLE_KEY));
            this.setSwitchRoleAccount(model.getString(SWITCH_ROLE_ACCOUNT_KEY));
            this.setSwitchRoleName(model.getString(SWITCH_ROLE_NAME_KEY));
        } else {
            this.setSwitchRole(false);
            this.setSwitchRoleAccount("");
            this.setSwitchRoleName("");
        }

        m_useAnonymous = model.getBoolean(USE_ANONYMOUS_KEY, false);

        // added in 4.1, use protocol as fallback
        m_serviceName = model.getString(CFG_SERVICE_NAME, getProtocol());
    }

    /**
     * Set whether Switch Role should be used.
     *
     * @param switchRole Whether Switch Role should be used
     */
    public void setSwitchRole(final boolean switchRole) {
        m_switchRole = switchRole;
    }

    /**
     * Set the Switch Role account that should be used.
     *
     * @param switchRoleAccount The Switch Role account that should be used
     */
    public void setSwitchRoleAccount(final String switchRoleAccount) {
        m_switchRoleAccount = switchRoleAccount;
    }

    /**
     * Set the Switch Role name that should be used.
     *
     * @param switchRoleName The Switch Role name that should be used
     */
    public void setSwitchRoleName(final String switchRoleName) {
        m_switchRoleName = switchRoleName;
    }

    /**
     * Set whether some key chain should be used when connecting
     *
     * @param use <code>true</code> if key chain should be used, <code>false</code> if not
     */
    public void setUseKeyChain(final boolean use) {
        m_useKeyChain = use;
    }

    /**
     * Returns whether the key chain should be used or not
     *isUseAnonymous
     * @return whether key chain should be used, <code>true</code> if it should be used, <code>false</code> if not
     */
    public boolean useKeyChain() {
        return m_useKeyChain;
    }

    /**
     * Set whether SSE should be used.
     *
     * @param use whether SSE should be used
     */
    public void setUseSSEncryption(final boolean use) {
        m_useSSEncryption = use;
    }

    /**
     * Returns whether SSE should be used.
     *
     * @return Whether SSE should be used
     */
    public boolean useSSEncryption() {
        return m_useSSEncryption;
    }

    @Override
    public void save(final ModelContentWO model) {
        super.save(model);
        model.addBoolean(KEY_CHAIN_KEY, m_useKeyChain);
        // New Server Side Encryption AP-8823
        model.addBoolean(SSE_KEY, m_useSSEncryption);

        // New Switch Role AP-11221
        model.addBoolean(SWITCH_ROLE_KEY, m_switchRole);
        model.addString(SWITCH_ROLE_ACCOUNT_KEY, m_switchRoleAccount);
        model.addString(SWITCH_ROLE_NAME_KEY, m_switchRoleName);

        model.addBoolean(USE_ANONYMOUS_KEY, m_useAnonymous);

        model.addString(CFG_SERVICE_NAME, m_serviceName);
    }

    public static CloudConnectionInformation load(final ModelContentRO model) throws InvalidSettingsException {
        return new CloudConnectionInformation(model);
    }

    /**
     * Returns whether a Switch Role should be used.
     *
     * @return Whether a Switch Role should be used
     */
    public boolean switchRole() {
        return m_switchRole;
    }

    /**
     * Returns the Switch Role account that should be used.
     *
     * @return The Switch Role account that should be used
     */
    public String getSwitchRoleAccount() {
        return m_switchRoleAccount;
    }

    /**
     * Returns the Switch Role name that should be used.
     *
     * @return The Switch Role name that should be used
     */
    public String getSwitchRoleName() {
        return m_switchRoleName;
    }

    /**
     * @return the useAnonymous
     */
    public boolean isUseAnonymous() {
        return m_useAnonymous;
    }

    /**
     * @param useAnonymous the useAnonymous to set
     */
    public void setUseAnonymous(final boolean useAnonymous) {
        m_useAnonymous = useAnonymous;
    }

    /**
     * Set a human readable service name.
     *
     * @param serviceName human readable service name
     */
    public void setServiceName(final String serviceName) {
        m_serviceName = serviceName;
    }

    /**
     * @return human readable service name
     */
    public String getServiceName() {
        return m_serviceName;
    }
}
