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
package org.knime.cloud.aws.mlservices.nodes.comprehend.language;

import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import org.knime.cloud.aws.mlservices.nodes.comprehend.BaseComprehendOperation;
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
import com.amazonaws.services.comprehend.model.DetectDominantLanguageRequest;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageResult;
import com.amazonaws.services.comprehend.model.DominantLanguage;

/**
 * Implementation of the operation to obtain the dominant languages of each input document.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class LanguageOperation extends BaseComprehendOperation {

    /**
     * Creates a new instance of {@code LanguageOperation} with given parameters.
     *
     * @param cxnInfo The {@code CloudConnectionInformation}
     * @param textColumnName The name of the text column
     * @param outputSpec The output table spec
     */
    LanguageOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final DataTableSpec outputSpec) {
        super(cxnInfo, textColumnName, outputSpec);
    }

    @Override
    public void compute(final RowInput in, final RowOutput out, final AmazonComprehend comprehendClient,
        final int textColIdx, final ExecutionContext exec, final long rowCount)
        throws CanceledExecutionException, InterruptedException {

        long inputRowIndex = 0;
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Comprehend to
        // detect the languages.
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
                Stream.generate(DataType::getMissingCell).limit(numInputColumns + 3).toArray(DataCell[]::new);
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
                final DetectDominantLanguageRequest detectDominantLanguageRequest =
                    new DetectDominantLanguageRequest().withText(textValue);

                final DetectDominantLanguageResult detectDominantLanguageResult =
                    comprehendClient.detectDominantLanguage(detectDominantLanguageRequest);

                final List<DominantLanguage> dominantLangs = detectDominantLanguageResult.getLanguages();

                // Push rows (one per language) to the output.
                for (int index = 0; index < dominantLangs.size(); index++) {
                    final DominantLanguage dominantLang = dominantLangs.get(index);

                    // Copy the results to the new columns in the output.
                    cells[numInputColumns] = new StringCell(code2Name(dominantLang.getLanguageCode()));
                    cells[numInputColumns + 1] = new StringCell(dominantLang.getLanguageCode());
                    cells[numInputColumns + 2] = new DoubleCell(dominantLang.getScore());

                    // Make row key unique with the input row number and the sequence number of each
                    // token
                    final RowKey key = new RowKey("Row " + inputRowIndex + "_" + index);

                    // Create a new data row and push it to the output container.
                    final DataRow row = new DefaultRow(key, cells);
                    out.push(row);
                }
            } else {
                final RowKey key = new RowKey("Row " + inputRowIndex + "_" + 0);
                final DataRow row = new DefaultRow(key, cells);
                out.push(row);
            }
            ++inputRowIndex;
        }
    }

    /**
     * Converts a language code to the name of a language while the name is represented in the users' system language.
     *
     * @param langCode The language code
     * @return The name of the language
     */
    private static String code2Name(final String langCode) {
        final String[] codes = langCode.split("-");
        if (codes.length == 1) {
            final Locale loc = new Locale(codes[0]);
            return loc.getDisplayLanguage();
        } else if (codes.length > 1) {
            final Locale loc = new Locale(codes[0], codes[1]);
            return loc.getDisplayLanguage();
        }

        return "<unknown>";
    }

}
