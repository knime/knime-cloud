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
 *   May 22, 2019 (Julian Bunzel): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend.tagging;

import org.knime.cloud.aws.mlservices.utils.comprehend.ComprehendUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.ext.textprocessing.data.DocumentValue;
import org.knime.ext.textprocessing.nodes.tokenization.TokenizerFactoryRegistry;

/**
 * General node dialog for Amazon Comprehend tagger nodes.
 *
 * @author Julian Bunzel, KNIME GmbH, Berlin, Germany
 */
public class ComprehendTaggerNodeDialog extends DefaultNodeSettingsPane {

    /**
     * A {@link SettingsModelBoolean} storing a boolean value that indicates whether to replace the document column or
     * not.
     */
    private final SettingsModelBoolean m_replaceDocModel = ComprehendTaggerNodeModel.getReplaceDocumentModel();

    /**
     * A {@link SettingsModelString} storing the name of the new column to create
     */
    private final SettingsModelString m_newDocumentColModel = ComprehendTaggerNodeModel.getNewDocumentColumnModel();

    /**
     * The input data table spec
     */
    private DataTableSpec m_spec;

    /**
     * Creates a new instance of {@code ComprehendTaggerNodeDialog}.
     */
    @SuppressWarnings("unchecked")
    public ComprehendTaggerNodeDialog() {
        super();
        createNewGroup("Column settings");
        addDialogComponent(new DialogComponentColumnNameSelection(ComprehendUtils.getTextColumnNameModel(),
            "Document column", 1, DocumentValue.class));
        setHorizontalPlacement(true);

        final DialogComponentBoolean replaceComp = new DialogComponentBoolean(m_replaceDocModel, "Replace column");
        replaceComp.setToolTipText("Replace selected document column");
        m_replaceDocModel.addChangeListener(e -> updateSettings());
        addDialogComponent(replaceComp);

        final DialogComponentString newDocColNameComp =
            new DialogComponentString(m_newDocumentColModel, "Append column", true, 20);
        newDocColNameComp.setToolTipText("Name of the new document column");
        addDialogComponent(newDocColNameComp);
        setHorizontalPlacement(false);

        createNewGroup("Tagger settings");
        setHorizontalPlacement(false);
        addDialogComponent(new DialogComponentStringSelection(ComprehendUtils.getSourceLanguageModel(),
            "Source language", ComprehendUtils.LANG_MAP.keySet()));
        addDialogComponent(new DialogComponentStringSelection(ComprehendTaggerNodeModel.getTokenizerModel(),
            "Word tokenizer", TokenizerFactoryRegistry.getTokenizerFactoryMap().keySet()));
        updateSettings();
    }

    /**
     * Enables/disables the new document column text component, if the replace document column is activated/deactivated.
     */
    private final void updateSettings() {
        m_newDocumentColModel.setEnabled(!m_replaceDocModel.getBooleanValue());
    }

    @Override
    public void loadAdditionalSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_spec = (DataTableSpec)specs[1];
    }

    @Override
    public void saveAdditionalSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        if (!m_replaceDocModel.getBooleanValue()) {
            final String newColName = m_newDocumentColModel.getStringValue();
            if (newColName.isEmpty()) {
                throw new InvalidSettingsException("The name of the new column cannot be empty.");
            }
            if (m_spec.containsName(newColName)) {
                throw new InvalidSettingsException(
                    "Can't create new column \"" + newColName + "\" as input spec already contains such column!");
            }
        }
    }
}
