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

import org.knime.cloud.aws.mlservices.nodes.comprehend.BaseComprehendLangNodeModel;
import org.knime.cloud.aws.mlservices.nodes.comprehend.ComprehendOperation;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.data.DocumentCell;
import org.knime.ext.textprocessing.data.DocumentValue;
import org.knime.ext.textprocessing.util.ColumnSelectionVerifier;

/**
 * Abstract node model for the Amazon Comprehend tagger nodes.
 *
 * @author Julian Bunzel, KNIME GmbH, Berlin, Germany
 */
public abstract class ComprehendTaggerNodeModel extends BaseComprehendLangNodeModel {

    /** Configuration key for the new document column name */
    private static final String CFG_KEY_NEW_DOCUMENT_COL = "new_document_column";

    /** Configuration key for the replace document option */
    private static final String CFG_KEY_REPLACE_DOC = "replace_document_column";

    /** Configuration key for the word tokenizer selection */
    private static final String CFG_KEY_TOKENIZER = "word_tokenizer";

    /** Default value for the new document column name */
    private static final String DEF_NEW_DOCUMENT_COL = "Tagged documents";

    /** Default value for the word tokenizer */
    private static final String DEF_WORD_TOKENIZER = "OpenNLP English WordTokenizer";

    /** Default value for the replace document option */
    private static final boolean DEF_REPLACE_DOC = true;

    /**
     * Creates and returns the string settings model containing the name of the column with the new, tagged documents.
     *
     * @return Returns the string settings model containing the name of the column with the new, tagged documents.
     */
    static SettingsModelString getNewDocumentColumnModel() {
        return new SettingsModelString(CFG_KEY_NEW_DOCUMENT_COL, DEF_NEW_DOCUMENT_COL);
    }

    /**
     * Creates and returns the boolean settings model which contains the flag if the old documents have to be replaced
     * by the tagged documents or if they will be appended.
     *
     * @return Returns the boolean settings model which contains the flag if the old documents have to be replaced by
     *         the tagged documents or if they will be appended.
     */
    static SettingsModelBoolean getReplaceDocumentModel() {
        return new SettingsModelBoolean(CFG_KEY_REPLACE_DOC, DEF_REPLACE_DOC);
    }

    /**
     * Creates and returns the settings model, storing the name of the word tokenizer.
     *
     * @return The settings model with the name of the word tokenizer.
     */
    static final SettingsModelString getTokenizerModel() {
        return new SettingsModelString(CFG_KEY_TOKENIZER, DEF_WORD_TOKENIZER);
    }

    /** {@link SettingsModelString} storing the name of the new document column to create */
    private final SettingsModelString m_newDocColumn = getNewDocumentColumnModel();

    /** {@link SettingsModelBoolean} storing the boolean value whether to replace the original document column or not */
    private final SettingsModelBoolean m_replaceDoc = getReplaceDocumentModel();

    /** {@link SettingsModelString} storing the name of the word tokenizer */
    private final SettingsModelString m_tokenizerModel = getTokenizerModel();

    @Override
    protected void checkDataTableSpec(final DataTableSpec inputSpec) throws InvalidSettingsException {
        // verify that the incoming DataTableSpec contains the minimum number of document columns
        final long numDocCols = inputSpec.stream()//
            .filter(colSpec -> colSpec.getType().isCompatible(DocumentValue.class))//
            .count();
        if (numDocCols < 1) {
            throw new InvalidSettingsException(
                "There has to be at least one column containing DocumentCells!");
        }

        // Verify document columns and auto-guess columns
        ColumnSelectionVerifier.verifyColumn(getTextColumnModel(), inputSpec, DocumentValue.class, null)
            .ifPresent(msg -> setWarningMessage(msg));
        if (!m_replaceDoc.getBooleanValue()) {
            final String newColName = m_newDocColumn.getStringValue().trim();
            if (newColName.isEmpty()) {
                throw new InvalidSettingsException("The name of the new column cannot be empty.");
            }
            if (inputSpec.containsName(newColName)) {
                throw new InvalidSettingsException(
                    "Can't create new column \"" + newColName + "\" as input spec already contains such column!");
            }
        }
    }

    @Override
    protected DataTableSpec generateOutputTableSpec(final DataTableSpec inputSpec) {
        // The columns added from the AWS call.
        final DataColumnSpec[] allColSpecs = new DataColumnSpec[1];
        allColSpecs[0] = new DataColumnSpecCreator(m_newDocColumn.getStringValue(), DocumentCell.TYPE).createSpec();
        return !m_replaceDoc.getBooleanValue() ? new DataTableSpec(inputSpec, new DataTableSpec(allColSpecs))
            : inputSpec;
    }

    @Override
    protected final ComprehendOperation getOperationInstance(final CloudConnectionInformation cxnInfo,
        final DataTableSpec outputSpec, final String textColumn, final String sourceLanguage) {
        return getOperationInstance(cxnInfo, outputSpec, textColumn, sourceLanguage, m_tokenizerModel.getStringValue(),
            !m_replaceDoc.getBooleanValue() ? m_newDocColumn.getStringValue() : null);
    }

    /**
     * Creates and returns an instance of a specific implementation of {@link ComprehendTaggerOperation}.
     *
     * @param cxnInfo The connection information
     * @param outputSpec The output data table spec
     * @param textColumn The name of the text column
     * @param sourceLanguage The source language
     * @param tokenizerName The name of the word tokenizer
     * @param newColName The name of the new document column.{@code null} if original document column is replaced
     * @return New instance of a specific {@code ComprehendTaggerOperation} implementation
     */
    protected abstract ComprehendTaggerOperation getOperationInstance(final CloudConnectionInformation cxnInfo,
        final DataTableSpec outputSpec, final String textColumn, final String sourceLanguage,
        final String tokenizerName, final String newColName);

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        m_tokenizerModel.saveSettingsTo(settings);
        m_newDocColumn.saveSettingsTo(settings);
        m_replaceDoc.saveSettingsTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadValidatedSettingsFrom(settings);
        m_tokenizerModel.loadSettingsFrom(settings);
        m_newDocColumn.loadSettingsFrom(settings);
        m_replaceDoc.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettings(settings);
        m_tokenizerModel.validateSettings(settings);
        m_newDocColumn.validateSettings(settings);
        m_replaceDoc.validateSettings(settings);
    }
}
