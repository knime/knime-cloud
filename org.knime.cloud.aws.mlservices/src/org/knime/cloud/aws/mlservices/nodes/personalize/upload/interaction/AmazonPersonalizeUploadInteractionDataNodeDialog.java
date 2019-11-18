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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.cloud.aws.mlservices.nodes.personalize.upload.AbstractAmazonPersonalizeDataUploadNodeDialog;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnSelectionComboxBox;
import org.knime.core.node.util.ColumnSelectionPanel;
import org.knime.core.node.util.DataValueColumnFilter;

/**
 * Node dialog for Amazon Personalize interaction data upload node.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class AmazonPersonalizeUploadInteractionDataNodeDialog
    extends AbstractAmazonPersonalizeDataUploadNodeDialog<AmazonPersonalizeUploadInteractionDataNodeSettings> {

    private ColumnSelectionComboxBox m_columnSelectionUserID;

    private ColumnSelectionComboxBox m_columnSelectionItemID;

    private ColumnSelectionComboxBox m_columnSelectionTimestamp;

    private ColumnSelectionPanel m_columnSelectionEventType;

    private ColumnSelectionPanel m_columnSelectionEventValue;

    private boolean m_loaded = false;

    @SuppressWarnings("unchecked")
    @Override
    protected JPanel layoutRequiredColumnMapping() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.fill = GridBagConstraints.NONE;
        // user id column selection
        panel.add(new JLabel("User ID column"), gbc);
        m_columnSelectionUserID = new ColumnSelectionComboxBox(BorderFactory.createEmptyBorder(), StringValue.class);
        m_columnSelectionUserID.addActionListener(l -> updateFilterPanel());
        gbc.gridx++;
        panel.add(m_columnSelectionUserID, gbc);
        // item id column selection
        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(new JLabel("Item ID column"), gbc);
        m_columnSelectionItemID = new ColumnSelectionComboxBox(BorderFactory.createEmptyBorder(), StringValue.class);
        m_columnSelectionItemID.addActionListener(l -> updateFilterPanel());
        gbc.gridx++;
        panel.add(m_columnSelectionItemID, gbc);
        // timestamp column selection
        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(new JLabel("Timestamp column"), gbc);
        m_columnSelectionTimestamp = new ColumnSelectionComboxBox(BorderFactory.createEmptyBorder(), LongValue.class);
        m_columnSelectionTimestamp.addActionListener(l -> updateFilterPanel());
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_columnSelectionTimestamp, gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected JPanel layoutOptionalColumnMapping() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.fill = GridBagConstraints.NONE;
        // event type column selection
        panel.add(new JLabel("Event type column"), gbc);
        m_columnSelectionEventType = new ColumnSelectionPanel(BorderFactory.createEmptyBorder(),
            new DataValueColumnFilter(StringValue.class), true);
        m_columnSelectionEventType.addActionListener(l -> updateFilterPanel());
        gbc.gridx++;
        panel.add(m_columnSelectionEventType, gbc);
        // event value column selection
        gbc.gridy++;
        gbc.gridx = 0;
        panel.add(new JLabel("Event value column"), gbc);
        m_columnSelectionEventValue = new ColumnSelectionPanel(BorderFactory.createEmptyBorder(),
            new DataValueColumnFilter(DoubleValue.class), true);
        m_columnSelectionEventValue.addActionListener(l -> updateFilterPanel());
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_columnSelectionEventValue, gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setUserIDColumnName(m_columnSelectionUserID.getSelectedColumn());
        m_settings.setItemIDColumnName(m_columnSelectionItemID.getSelectedColumn());
        m_settings.setTimestampColumnName(m_columnSelectionTimestamp.getSelectedColumn());
        m_settings.setEventTypeColumnName(m_columnSelectionEventType.getSelectedColumn());
        m_settings.setEventValueColumnName(m_columnSelectionEventValue.getSelectedColumn());
        if (!m_settings.isColumnSelectionIsDistinct()) {
            throw new InvalidSettingsException("The required and optional column selections must be distinct.");
        }
        super.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_loaded = false;
        final DataTableSpec spec = (DataTableSpec)specs[1];
        m_columnSelectionUserID.update(spec, m_settings.getUserIDColumnName());
        m_columnSelectionItemID.update(spec, m_settings.getItemIDColumnName());
        m_columnSelectionTimestamp.update(spec, m_settings.getTimestampColumnName());
        m_columnSelectionEventType.update(spec, m_settings.getEventTypeColumnName());
        m_columnSelectionEventValue.update(spec, m_settings.getEventValueColumnName());
        m_loaded = true;
        updateFilterPanel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmazonPersonalizeUploadInteractionDataNodeSettings createSettings() {
        if (m_settings != null) {
            return m_settings;
        }
        return new AmazonPersonalizeUploadInteractionDataNodeSettings();
    }

    private void updateFilterPanel() {
        if (m_loaded) {
            final DataColumnSpec userIDSpec = (DataColumnSpec)m_columnSelectionUserID.getSelectedItem();
            final DataColumnSpec itemIDSpec = (DataColumnSpec)m_columnSelectionItemID.getSelectedItem();
            final DataColumnSpec timestampSpec = (DataColumnSpec)m_columnSelectionTimestamp.getSelectedItem();
            final DataColumnSpec eventTypeSpec = m_columnSelectionEventType.getSelectedColumnAsSpec();
            final DataColumnSpec eventValueSpec = m_columnSelectionEventValue.getSelectedColumnAsSpec();
            m_columnFilterPanel.resetHiding();
            m_columnFilterPanel.hideNames(itemIDSpec, userIDSpec, timestampSpec);
            if (eventTypeSpec != null) {
                m_columnFilterPanel.hideNames(eventTypeSpec);
            }
            if (eventValueSpec != null) {
                m_columnFilterPanel.hideNames(eventValueSpec);
            }
        }
    }
}
