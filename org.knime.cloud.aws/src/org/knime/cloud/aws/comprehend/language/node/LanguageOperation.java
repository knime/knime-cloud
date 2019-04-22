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
package org.knime.cloud.aws.comprehend.language.node;

import java.util.Locale;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.cloud.aws.comprehend.BaseComprehendOperation;
import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentCell;
import org.knime.ext.textprocessing.data.DocumentValue;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageRequest;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageResult;
import com.amazonaws.services.comprehend.model.DominantLanguage;

/**
 *
 * @author jfalgout
 */
class LanguageOperation extends BaseComprehendOperation {

    LanguageOperation(final ConnectionInformation cxnInfo, final String textColumnName, final DataTableSpec outputSpec) {
        super(cxnInfo, textColumnName, outputSpec);
    }

    @Override
    public void compute(final RowInput in, final RowOutput out, final ExecutionContext exec, final long rowCount) throws CanceledExecutionException, InterruptedException {

        // Create a connection to the Comprehend service in the provided region
        AmazonComprehend comprehendClient = ComprehendUtils.getClient(m_cxnInfo);

        // Access the input data table
        int textColumnIdx = in.getDataTableSpec().findColumnIndex(m_textColumnName);
        long inputRowIndex = 0;
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Comprehend to detect the languages.
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {
            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double) rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }

            // Grab the text to evaluate
            Document inputDoc = ((DocumentValue) inputRow.getCell(textColumnIdx)).getDocument();
            String textValue = inputDoc.getDocumentBodyText();

            DetectDominantLanguageRequest detectDominantLanguageRequest =
                    new DetectDominantLanguageRequest()
                        .withText(textValue);

            DetectDominantLanguageResult detectDominantLanguageResult =
                comprehendClient
                    .detectDominantLanguage(detectDominantLanguageRequest);

            long outputRowIndex = 0;
            for (DominantLanguage dominantLang : detectDominantLanguageResult.getLanguages()) {

                // Create cells containing the output data.
                // Copy the input field values to the output.
                int numInputColumns = m_outputTableSpec.getNumColumns();
                DataCell[] cells = new DataCell[numInputColumns + 8];
                for (int i = 0; i < numInputColumns; i++) {
                    cells[i] = inputRow.getCell(i);
                }

                // Copy the results to the new columns in the output.
                cells[numInputColumns] = new DocumentCell(inputDoc);
                cells[numInputColumns + 1] = new StringCell(code2Name(dominantLang.getLanguageCode()));
                cells[numInputColumns + 2] = new StringCell(dominantLang.getLanguageCode());
                cells[numInputColumns + 3] = new DoubleCell(dominantLang.getScore());

                // Make row key unique with the input row number and the sequence number of each token
                RowKey key = new RowKey("Row " + inputRowIndex + "_" + outputRowIndex++);

                // Create a new data row and push it to the output container.
                DataRow row = new DefaultRow(key, cells);
                out.push(row);
            }

            ++inputRowIndex;
        }

        return;
    }

    private static String code2Name(final String langCode) {
        String[] codes = langCode.split("-");
        if (codes.length == 1) {
            Locale loc = new Locale(codes[0]);
            return loc.getDisplayLanguage();
        }
        else if (codes.length > 1){
            Locale loc = new Locale(codes[0], codes[1]);
            return loc.getDisplayLanguage();
        }

        return "<unknown>";
    }

}
