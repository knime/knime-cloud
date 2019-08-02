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
 *   Apr 12, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.personalize.recommend;

import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * The node dialog for the Amazon Translate node.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class RecommendNodeDialog extends DefaultNodeSettingsPane {

    /** Settings model storing the source column containing entity id's. */
    final SettingsModelString m_entityCol = RecommendNodeModel.getEntityColModel();

    /** Settings model storing the source language. */
    final SettingsModelString m_campaignArn = RecommendNodeModel.getCampaignArnModel();

    @SuppressWarnings("unchecked")
    protected RecommendNodeDialog() {
        super();
        addDialogComponent(new DialogComponentColumnNameSelection(
            RecommendNodeModel.getEntityColModel(),
            "Column containing entity id's:",
            1,
            StringValue.class));

        addDialogComponent(new DialogComponentString(
            RecommendNodeModel.getCampaignArnModel(),
            "Campaign ARN:",
            true,
            20));

        addDialogComponent(new DialogComponentButtonGroup(
            RecommendNodeModel.getRecTypeModel(),
            "",
            true,
            new ButtonGroupEnumInterface[] { new UserRecommendation(), new ItemRecommendation() }
            ));

        addDialogComponent(new DialogComponentNumber(
            RecommendNodeModel.getOutputLimitModel(),
            "Number of results (0 implies unlimited): ",
            5,
            5));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {

    }

    private class UserRecommendation implements ButtonGroupEnumInterface {

        @Override
        public String getText() {
            return "User personalization: recommend items for users";
        }

        @Override
        public String getActionCommand() {
            return RecommendNodeModel.REC_TYPE_USER;
        }

        @Override
        public String getToolTip() {
            return "Select this option to recommend items for a user. The input column must contain user identifiers.";
        }

        @Override
        public boolean isDefault() {
            return true;
        }

    }

    private class ItemRecommendation implements ButtonGroupEnumInterface {

        @Override
        public String getText() {
            return "Related items: recommend items related to the given item";
        }

        @Override
        public String getActionCommand() {
            return RecommendNodeModel.REC_TYPE_ITEM;
        }

        @Override
        public String getToolTip() {
            // TODO Auto-generated method stub
            return "Select this option to find a list of items related to the given item. The input column must container item identifiers.";
        }

        @Override
        public boolean isDefault() {
            return false;
        }

    }
}
