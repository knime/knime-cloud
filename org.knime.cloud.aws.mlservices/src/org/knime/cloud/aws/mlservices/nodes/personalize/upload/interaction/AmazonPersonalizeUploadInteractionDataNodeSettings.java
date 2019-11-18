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
 *   Oct 30, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.upload.interaction;

import java.util.HashSet;

import org.knime.cloud.aws.mlservices.nodes.personalize.upload.AbstractAmazonPersonalizeDataUploadNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Node settings for Amazon Personalize interaction data upload node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeUploadInteractionDataNodeSettings extends AbstractAmazonPersonalizeDataUploadNodeSettings {

    private static final String CFG_KEY_USERID_COLUMN_NAME = "userID_column_name";

    private static final String CFG_KEY_ITEMID_COLUMN_NAME = "itemID_column_name";

    private static final String CFG_KEY_TIMESTAMP_COLUMN_NAME = "timestamp_column_name";

    private static final String CFG_KEY_EVENT_TYPE_COLUMN_NAME = "eventType_column_name";

    private static final String CFG_KEY_EVENT_VALUE_COLUMN_NAME = "eventValue_column_name";

    private static final String DEF_USERID_COLUMN_NAME = "USER_ID";

    private static final String DEF_ITEMID_COLUMN_NAME = "ITEM_ID";

    private static final String DEF_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";

    private static final String DEF_EVENT_TYPE_COLUMN_NAME = null;

    private static final String DEF_EVENT_VALUE_COLUMN_NAME = null;

    private String m_userIDColumnName = DEF_USERID_COLUMN_NAME;

    private String m_itemIDColumnName = DEF_ITEMID_COLUMN_NAME;

    private String m_timestampColumnName = DEF_TIMESTAMP_COLUMN_NAME;

    private String m_eventTypeColumnName = DEF_EVENT_TYPE_COLUMN_NAME;

    private String m_eventValueColumnName = DEF_EVENT_VALUE_COLUMN_NAME;

    /**
     * @return the userIDColumnName
     */
    public String getUserIDColumnName() {
        return m_userIDColumnName;
    }

    /**
     * @param userIDColumnName the userIDColumnName to set
     */
    public void setUserIDColumnName(final String userIDColumnName) {
        m_userIDColumnName = userIDColumnName;
    }

    /**
     * @return the itemIDColumnName
     */
    public String getItemIDColumnName() {
        return m_itemIDColumnName;
    }

    /**
     * @param itemIDColumnName the itemIDColumnName to set
     */
    public void setItemIDColumnName(final String itemIDColumnName) {
        m_itemIDColumnName = itemIDColumnName;
    }

    /**
     * @return the timestampColumnName
     */
    public String getTimestampColumnName() {
        return m_timestampColumnName;
    }

    /**
     * @param timestampColumnName the timestampColumnName to set
     */
    public void setTimestampColumnName(final String timestampColumnName) {
        m_timestampColumnName = timestampColumnName;
    }

    /**
     * @return the eventTypeColumnName
     */
    public String getEventTypeColumnName() {
        return m_eventTypeColumnName;
    }

    /**
     * @param eventTypeColumnName the eventTypeColumnName to set
     */
    public void setEventTypeColumnName(final String eventTypeColumnName) {
        m_eventTypeColumnName = eventTypeColumnName;
    }

    /**
     * @return the eventValueColumnName
     */
    public String getEventValueColumnName() {
        return m_eventValueColumnName;
    }

    /**
     * @param eventValueColumnName the eventValueColumnName to set
     */
    public void setEventValueColumnName(final String eventValueColumnName) {
        m_eventValueColumnName = eventValueColumnName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettings(settings);
        m_itemIDColumnName = settings.getString(CFG_KEY_ITEMID_COLUMN_NAME);
        m_userIDColumnName = settings.getString(CFG_KEY_USERID_COLUMN_NAME);
        m_timestampColumnName = settings.getString(CFG_KEY_TIMESTAMP_COLUMN_NAME);
        m_eventTypeColumnName = settings.getString(CFG_KEY_EVENT_TYPE_COLUMN_NAME);
        m_eventValueColumnName = settings.getString(CFG_KEY_EVENT_VALUE_COLUMN_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettingsForDialog(final NodeSettingsRO settings, final DataTableSpec spec) {
        super.loadSettingsForDialog(settings, spec);
        m_itemIDColumnName = settings.getString(CFG_KEY_ITEMID_COLUMN_NAME, DEF_ITEMID_COLUMN_NAME);
        m_userIDColumnName = settings.getString(CFG_KEY_USERID_COLUMN_NAME, DEF_USERID_COLUMN_NAME);
        m_timestampColumnName = settings.getString(CFG_KEY_TIMESTAMP_COLUMN_NAME, DEF_TIMESTAMP_COLUMN_NAME);
        m_eventTypeColumnName = settings.getString(CFG_KEY_EVENT_TYPE_COLUMN_NAME, DEF_EVENT_TYPE_COLUMN_NAME);
        m_eventValueColumnName = settings.getString(CFG_KEY_EVENT_VALUE_COLUMN_NAME, DEF_EVENT_VALUE_COLUMN_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings) {
        super.saveSettings(settings);
        settings.addString(CFG_KEY_ITEMID_COLUMN_NAME, m_itemIDColumnName);
        settings.addString(CFG_KEY_USERID_COLUMN_NAME, m_userIDColumnName);
        settings.addString(CFG_KEY_TIMESTAMP_COLUMN_NAME, m_timestampColumnName);
        settings.addString(CFG_KEY_EVENT_TYPE_COLUMN_NAME, m_eventTypeColumnName);
        settings.addString(CFG_KEY_EVENT_VALUE_COLUMN_NAME, m_eventValueColumnName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getDatasetType() {
        return AmazonPersonalizeUploadInteractionDataNodeModel.DATATYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNumRequiredColumns() {
        return 3;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getNumSelectedOptionalColumns() {
        return (m_eventTypeColumnName == null ? 0 : 1) + (m_eventValueColumnName == null ? 0 : 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int getMinMetadataColumns() {
        return 0;
    }

    boolean isColumnSelectionIsDistinct() {
        final HashSet<String> hashSet = new HashSet<>();
        hashSet.add(m_userIDColumnName);
        hashSet.add(m_itemIDColumnName);
        hashSet.add(m_timestampColumnName);
        if (m_eventTypeColumnName != null) {
            hashSet.add(m_eventTypeColumnName);
        }
        if (m_eventValueColumnName != null) {
            hashSet.add(m_eventValueColumnName);
        }
        return hashSet.size() == getNumRequiredColumns() + getNumSelectedOptionalColumns();
    }

}
