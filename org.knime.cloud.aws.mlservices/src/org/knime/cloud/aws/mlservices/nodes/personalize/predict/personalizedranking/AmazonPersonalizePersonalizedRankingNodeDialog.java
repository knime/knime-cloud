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
package org.knime.cloud.aws.mlservices.nodes.personalize.predict.personalizedranking;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.cloud.aws.mlservices.nodes.personalize.predict.AbstractAmazonPersonalizePredictNodeDialog;
import org.knime.cloud.aws.mlservices.nodes.personalize.predict.AmazonPersonalizePredictNodeSettings;
import org.knime.cloud.aws.mlservices.utils.personalize.RecipeType;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.collection.ListDataValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnSelectionPanel;

/**
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
public class AmazonPersonalizePersonalizedRankingNodeDialog
    extends AbstractAmazonPersonalizePredictNodeDialog<AmazonPersonalizePredictNodeSettings> {

    private ColumnSelectionPanel m_columSelectionItems;

    /**
     * {@inheritDoc}
     */
    @Override
    protected JPanel layoutAdditionalSettings() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        panel.add(new JLabel("Item list column"), gbc);
        gbc.gridx++;
        m_columSelectionItems = new ColumnSelectionPanel(BorderFactory.createEmptyBorder(), ListDataValue.class);
        panel.add(m_columSelectionItems, gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        // Dummy label
        panel.add(new JLabel(), gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setItemsCol(m_columSelectionItems.getSelectedColumn());
        super.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_columSelectionItems.update((DataTableSpec)specs[1], m_settings.getItemsCol());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmazonPersonalizePredictNodeSettings getSettings() {
        return new AmazonPersonalizePredictNodeSettings();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isItemIDUsed() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isUserIDUsed() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isItemIDRequired() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecipeType getRecipeType() {
        return RecipeType.PERSONALIZED_RANKING;
    }

}
