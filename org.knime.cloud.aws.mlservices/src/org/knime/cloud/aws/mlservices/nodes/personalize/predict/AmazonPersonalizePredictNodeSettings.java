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
 *   Nov 2, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.predict;

import org.knime.cloud.aws.mlservices.utils.personalize.NameArnPair;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * The abstract node settings of the Amazon Personalize prediction nodes.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public class AmazonPersonalizePredictNodeSettings {

    private static final String CFG_KEY_CAMPAIGN = "campaign";

    private static final String CFG_KEY_USERID = "userID_column";

    private static final String CFG_KEY_ITEMID = "itemID_column";

    private static final String CFG_KEY_ITEM_LIST_COL = "items_column";

    private static final String CFG_KEY_MISSING_VALUE_HANDLING = "missing_value_handling";

    private static final NameArnPair DEF_CAMPAIGN = null;

    private static final String DEF_USERID = null;

    private static final String DEF_ITEMID = null;

    private static final String DEF_ITEM_LIST_COL = null;

    private static final MissingValueHandling DEF_MISSING_VALUE_HANDLING = MissingValueHandling.FAIL;

    private NameArnPair m_campaign = DEF_CAMPAIGN;

    private String m_userIDCol = DEF_USERID;

    private String m_itemIDCol = DEF_ITEMID;

    private String m_itemsCol = DEF_ITEM_LIST_COL;

    private MissingValueHandling m_missingValueHandling = DEF_MISSING_VALUE_HANDLING;

    /**
     * @return the campaign
     */
    public NameArnPair getCampaign() {
        return m_campaign;
    }

    /**
     * @param campaign the campaign to set
     */
    public void setCampaign(final NameArnPair campaign) {
        m_campaign = campaign;
    }

    /**
     * @return the userIDCol
     */
    public String getUserIDCol() {
        return m_userIDCol;
    }

    /**
     * @param userIDCol the userIDCol to set
     */
    public void setUserIDCol(final String userIDCol) {
        m_userIDCol = userIDCol;
    }

    /**
     * @return the itemIDCol
     */
    public String getItemIDCol() {
        return m_itemIDCol;
    }

    /**
     * @param itemIDCol the itemIDCol to set
     */
    public void setItemIDCol(final String itemIDCol) {
        m_itemIDCol = itemIDCol;
    }

    /**
     * @return the itemsCol
     */
    public String getItemsCol() {
        return m_itemsCol;
    }

    /**
     * @param itemsCol the itemsCol to set
     */
    public void setItemsCol(final String itemsCol) {
        m_itemsCol = itemsCol;
    }

    /**
     * @return the missingValueHandling
     */
    public MissingValueHandling getMissingValueHandling() {
        return m_missingValueHandling;
    }

    /**
     * @param missingValueHandling the missingValueHandling to set
     */
    public void setMissingValueHandling(final MissingValueHandling missingValueHandling) {
        m_missingValueHandling = missingValueHandling;
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if some settings are missing
     */
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_campaign = NameArnPair.loadSettings(settings, CFG_KEY_CAMPAIGN);
        m_userIDCol = settings.getString(CFG_KEY_USERID);
        m_itemIDCol = settings.getString(CFG_KEY_ITEMID);
        m_itemsCol = settings.getString(CFG_KEY_ITEM_LIST_COL);
        m_missingValueHandling = MissingValueHandling.valueOf(settings.getString(CFG_KEY_MISSING_VALUE_HANDLING));
    }

    /**
     * Loads the settings from the node settings object using default values if some settings are missing.
     *
     * @param settings a node settings object
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_campaign = NameArnPair.loadSettingsForDialog(settings, CFG_KEY_CAMPAIGN, DEF_CAMPAIGN);
        m_userIDCol = settings.getString(CFG_KEY_USERID, DEF_USERID);
        m_itemIDCol = settings.getString(CFG_KEY_ITEMID, DEF_ITEMID);
        m_itemsCol = settings.getString(CFG_KEY_ITEM_LIST_COL, DEF_ITEM_LIST_COL);
        m_missingValueHandling = MissingValueHandling
            .valueOf(settings.getString(CFG_KEY_MISSING_VALUE_HANDLING, DEF_MISSING_VALUE_HANDLING.name()));
    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {
        NameArnPair.saveSettings(settings, CFG_KEY_CAMPAIGN, m_campaign);
        settings.addString(CFG_KEY_USERID, m_userIDCol);
        settings.addString(CFG_KEY_ITEMID, m_itemIDCol);
        settings.addString(CFG_KEY_ITEM_LIST_COL, m_itemsCol);
        settings.addString(CFG_KEY_MISSING_VALUE_HANDLING, m_missingValueHandling.name());
    }

}
