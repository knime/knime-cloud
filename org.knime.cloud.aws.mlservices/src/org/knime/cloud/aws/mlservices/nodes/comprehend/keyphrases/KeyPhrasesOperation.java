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
 *   Mar 16, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend.keyphrases;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.knime.cloud.aws.mlservices.nodes.comprehend.BaseComprehendOperation;
import org.knime.cloud.aws.mlservices.utils.comprehend.ComprehendUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentValue;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesItemResult;
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesRequest;
import com.amazonaws.services.comprehend.model.BatchDetectKeyPhrasesResult;
import com.amazonaws.services.comprehend.model.KeyPhrase;

/**
 * Support streaming the key phases discovery computation.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class KeyPhrasesOperation extends BaseComprehendOperation {

    /** Language of the source text to be analyzed */
    private final String m_sourceLanguage;

    /**
     * Creates a new instance of {@code KeyPhrasesOperation} with given parameters.
     *
     * @param cxnInfo The {@code CloudConnectionInformation}
     * @param textColumnName The name of the text column
     * @param sourceLanguage The source language of the text
     * @param outputSpec The output table spec
     */
    KeyPhrasesOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final String sourceLanguage, final DataTableSpec outputSpec) {
        super(cxnInfo, textColumnName, outputSpec);
        this.m_sourceLanguage = sourceLanguage;
    }

    @Override
    public void compute(final RowInput in, final RowOutput out, final AmazonComprehend comprehendClient,
        final int textColIdx, final ExecutionContext exec, final long rowCount)
        throws CanceledExecutionException, InterruptedException {

        // Row index
        long rowCounter = 0;
        final int numInputColumns = in.getDataTableSpec().getNumColumns();

        // Create row batches based on global batch size and process one batch in one request
        final List<DataRow> rowBatch = new ArrayList<>(ComprehendUtils.BATCH_SIZE);
        final List<String> texts = new ArrayList<>(ComprehendUtils.BATCH_SIZE);
        final Set<Integer> validRows = new HashSet<>(ComprehendUtils.BATCH_SIZE);
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {
            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double)rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }
            rowBatch.add(inputRow);
            final DataCell cell = inputRow.getCell(textColIdx);
            if (!cell.isMissing()) {
                String textValue = null;
                if (cell.getType().isCompatible(DocumentValue.class)) {
                    final Document doc = ((DocumentValue)cell).getDocument();
                    textValue = doc.getTitle() + " " + doc.getDocumentBodyText();
                } else {
                    textValue = cell.toString();
                }
                texts.add(textValue);
                validRows.add(rowBatch.size() - 1);
            }
            if (rowBatch.size() == ComprehendUtils.BATCH_SIZE) {
                processChunk(out, comprehendClient, numInputColumns, rowBatch, texts, validRows);
            }
        }

        // process remaining chunk
        processChunk(out, comprehendClient, numInputColumns, rowBatch, texts, validRows);
    }

    /**
     * Method to process one chunk with given texts.
     *
     * @param out RowOutput to push new rows to
     * @param comprehendClient Comprehend client to send the requests
     * @param numInputColumns Number of input columns
     * @param rowBatch List containing rows
     * @param texts Texts to process
     * @param validRows List containing indices of valid rows
     * @throws InterruptedException Thrown if execution is canceled
     */
    @SuppressWarnings("null")
    private final void processChunk(final RowOutput out, final AmazonComprehend comprehendClient,
        final int numInputColumns, final List<DataRow> rowBatch, final List<String> texts, final Set<Integer> validRows)
        throws InterruptedException {
        final BatchDetectKeyPhrasesRequest detectKeyPhrasesRequest;
        final BatchDetectKeyPhrasesResult detectKeyPhrasesResult;
        Iterator<BatchDetectKeyPhrasesItemResult> results = null;
        if (!texts.isEmpty()) {
            detectKeyPhrasesRequest = new BatchDetectKeyPhrasesRequest().withTextList(texts)
                .withLanguageCode(ComprehendUtils.LANG_MAP.getOrDefault(m_sourceLanguage, "en"));
            detectKeyPhrasesResult = comprehendClient.batchDetectKeyPhrases(detectKeyPhrasesRequest);
            results = detectKeyPhrasesResult.getResultList().iterator();
        }
        final DataCell[] cells = new DataCell[numInputColumns + 4];
        for (int i = 0; i < rowBatch.size(); i++) {
            final DataRow row = rowBatch.get(i);
            for (int j = 0; j < numInputColumns; j++) {
                cells[j] = row.getCell(j);
            }
            if (validRows.contains(i)) {
                long outputRowIndex = 0;
                for (final KeyPhrase keyPhrase : results.next().getKeyPhrases()) {
                    // Set new output cell values.
                    cells[numInputColumns] = new StringCell(keyPhrase.getText());
                    cells[numInputColumns + 1] = new DoubleCell(keyPhrase.getScore());
                    cells[numInputColumns + 2] = new IntCell(keyPhrase.getBeginOffset());
                    cells[numInputColumns + 3] = new IntCell(keyPhrase.getEndOffset());

                    // Create a new data row and push it to the output container.
                    out.push(new DefaultRow(new RowKey(row.getKey().getString() + "_" + outputRowIndex++), cells));
                }
            } else {
                Arrays.fill(cells, numInputColumns, numInputColumns + 4, DataType.getMissingCell());
                out.push(new DefaultRow(new RowKey(row.getKey().getString() + "_" + 0), cells));
            }
        }
        // Clean up
        rowBatch.clear();
        texts.clear();
        validRows.clear();
    }
}
