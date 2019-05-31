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
 *   Apr 8, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.mlservices.nodes.comprehend.sentiment;

import java.util.stream.Stream;

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
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentValue;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.amazonaws.services.comprehend.model.SentimentScore;

/**
 * Implementation of the operation to obtain the sentiment of each sentence in an input document.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class SentimentOperation extends BaseComprehendOperation {
    // Language of the source text to be analyzed
    private final String m_sourceLanguage;

    /**
     * Creates a new instance of {@code SentimentOperation} with given parameters.
     *
     * @param cxnInfo The {@code ConnectionInformation}
     * @param textColumnName The name of the text column
     * @param sourceLanguage The source language of the text
     * @param outputSpec The output table spec
     */
    SentimentOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final String sourceLanguage, final DataTableSpec outputSpec) {
        super(cxnInfo, textColumnName, outputSpec);
        this.m_sourceLanguage = sourceLanguage;
    }

    @Override
    public void compute(final RowInput in, final RowOutput out, final AmazonComprehend comprehendClient,
        final int textColIdx, final ExecutionContext exec, final long rowCount)
        throws CanceledExecutionException, InterruptedException {
        long inputRowIndex = 0;
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Comprehend
        // and push each of the syntax elements to the output.
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {
            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double)rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }

            // Grab the text to evaluate
            String textValue = null;
            final DataCell cell = inputRow.getCell(textColIdx);
            // Create cells containing the output data.
            // Copy the input data to the output
            final int numInputColumns = inputRow.getNumCells();
            DataCell[] cells =
                Stream.generate(DataType::getMissingCell).limit(numInputColumns + 5).toArray(DataCell[]::new);
            for (int i = 0; i < numInputColumns; i++) {
                cells[i] = inputRow.getCell(i);
            }
            if (!cell.isMissing()) {
                if (cell.getType().isCompatible(DocumentValue.class)) {
                    final Document doc = ((DocumentValue)cell).getDocument();
                    textValue = doc.getTitle() + " " + doc.getDocumentBodyText();
                } else {
                    textValue = cell.toString();
                }
                final DetectSentimentRequest detectSentimentRequest = new DetectSentimentRequest().withText(textValue)
                    .withLanguageCode(ComprehendUtils.LANG_MAP.getOrDefault(m_sourceLanguage, "en"));

                final DetectSentimentResult detectSentimentResult =
                    comprehendClient.detectSentiment(detectSentimentRequest);

                // Grab scores for each sentiment category.
                final SentimentScore score = detectSentimentResult.getSentimentScore();

                // Copy the results to the new columns in the output.
                cells[numInputColumns] = new StringCell(detectSentimentResult.getSentiment());
                cells[numInputColumns + 1] = new DoubleCell(score.getMixed());
                cells[numInputColumns + 2] = new DoubleCell(score.getPositive());
                cells[numInputColumns + 3] = new DoubleCell(score.getNeutral());
                cells[numInputColumns + 4] = new DoubleCell(score.getNegative());
            }
            out.push(new DefaultRow(new RowKey("Row " + inputRowIndex), cells));
            ++inputRowIndex;
        }
    }
}
