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
 *   May 27, 2019 (Julian Bunzel): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend.tagging;

import org.knime.cloud.aws.mlservices.nodes.comprehend.BaseComprehendOperation;
import org.knime.cloud.aws.mlservices.utils.comprehend.ComprehendUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentValue;
import org.knime.ext.textprocessing.nodes.tagging.DocumentTagger;
import org.knime.ext.textprocessing.util.TextContainerDataCellFactory;
import org.knime.ext.textprocessing.util.TextContainerDataCellFactoryBuilder;

import com.amazonaws.services.comprehend.AmazonComprehend;

/**
 * Abstract class wrapping common code of comprehend tagger operations.
 *
 * @author Julian Bunzel, KNIME GmbH, Berlin, Germany
 */
public abstract class ComprehendTaggerOperation extends BaseComprehendOperation {

    private final String m_sourceLanguage;

    private final String m_tokenizerName;

    private final String m_newColName;

    /**
     * Creates and returns an instance of a specific implementation of {@link ComprehendTaggerOperation}.
     *
     * @param cxnInfo The connection information
     * @param textColumnName The name of the text column
     * @param sourceLanguage The source language
     * @param tokenizerName The name of the word tokenizer
     * @param newColName The name of the new document column.{@code null} if original document column is replaced
     * @param outputTableSpec The output data table spec
     */
    public ComprehendTaggerOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final String sourceLanguage, final String tokenizerName, final String newColName,
        final DataTableSpec outputTableSpec) {
        super(cxnInfo, textColumnName, outputTableSpec);
        this.m_sourceLanguage = sourceLanguage;
        this.m_tokenizerName = tokenizerName;
        this.m_newColName = newColName;
    }

    @Override
    public final void compute(final RowInput in, final RowOutput out, final AmazonComprehend comprehendClient,
        final int textColIdx, final ExecutionContext exec, final long rowCount)
        throws CanceledExecutionException, InterruptedException {
        // Create the tagger that uses the detect entities capability of the Comprehend
        // service.
        final DocumentTagger tagger =
            getTagger(comprehendClient, ComprehendUtils.LANG_MAP.getOrDefault(m_sourceLanguage, "en"), m_tokenizerName);

        final TextContainerDataCellFactory docCellFactory =
            TextContainerDataCellFactoryBuilder.createDocumentCellFactory();
        docCellFactory.prepare(FileStoreFactory.createFileStoreFactory(exec));

        long inputRowIndex = 0;
        long rowCounter = 0;

        // Tag each input document
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {

            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double)rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }

            // Grab the text to evaluate
            final DataCell cell = inputRow.getCell(textColIdx);
            final DataCell newDataCell;
            if (!cell.isMissing()) {
                final Document outputDoc = tagger.tag(((DocumentValue)cell).getDocument());
                newDataCell = docCellFactory.createDataCell(outputDoc);
            } else {
                newDataCell = cell;
            }

            // Create cells containing the output data.
            // Copy the input data to the output
            final int numInputColumns = inputRow.getNumCells();
            final DataCell[] cells =
                m_newColName != null ? new DataCell[numInputColumns + 1] : new DataCell[numInputColumns];
            for (int i = 0; i < numInputColumns; i++) {
                cells[i] = inputRow.getCell(i);
            }

            // Copy the output document tagged with entities to the output
            cells[m_newColName != null ? numInputColumns : textColIdx] = newDataCell;

            // Create a new data row and push it to the output container.
            final RowKey key = new RowKey("Row " + inputRowIndex);
            final DataRow row = new DefaultRow(key, cells);
            out.push(row);
            ++inputRowIndex;
        }
    }

    /**
     * Creates and returns a new instance of a specific implementation of a {@link DocumentTagger}.
     *
     * @param comprehendClient The {@code AmazonComprehend} client used to call the service
     * @param languageCode The language code
     * @param tokenizerName The name of the word tokenizer
     * @return Returns a new instance of a specific implementation of a {@code DocumentTagger}
     */
    protected abstract DocumentTagger getTagger(final AmazonComprehend comprehendClient, final String languageCode,
        final String tokenizerName);
}
