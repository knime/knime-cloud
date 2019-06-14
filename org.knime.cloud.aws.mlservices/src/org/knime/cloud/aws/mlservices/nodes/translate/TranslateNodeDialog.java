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
package org.knime.cloud.aws.mlservices.nodes.translate;

import org.knime.core.data.StringValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * The node dialog for the Amazon Translate node.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class TranslateNodeDialog extends DefaultNodeSettingsPane {

    /** Settings model storing the source language. */
    final SettingsModelString m_sourceLanguageModel = TranslateNodeModel.getSourceLanguageModel();

    /** Settings model storing the target language. */
    final SettingsModelString m_targetLanguageModel = TranslateNodeModel.getTargetLanguageModel();

    /**
     * New pane for configuring MyExampleNode node dialog. This is just a suggestion to demonstrate possible default
     * dialog components.
     */
    @SuppressWarnings("unchecked")
    protected TranslateNodeDialog() {
        super();
        addDialogComponent(new DialogComponentColumnNameSelection(TranslateNodeModel.getTextColModel(),
            "Text column to translate:", 1, StringValue.class));
        addDialogComponent(new DialogComponentStringSelection(m_sourceLanguageModel, "Source language:",
            TranslateUtils.getSourceLanguageMap().keySet()));
        addDialogComponent(new DialogComponentStringSelection(m_targetLanguageModel, "Target language:",
            TranslateUtils.getTargetLanguageMap().keySet()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        TranslateUtils.checkPair(m_sourceLanguageModel.getStringValue(), m_targetLanguageModel.getStringValue());
    }
}
