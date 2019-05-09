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
package org.knime.cloud.aws.nodes.translate;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.DocumentValue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

/**
 *
 * @author KNIME AG
 */
public class TranslateOperation {
    private final ConnectionInformation m_cxnInfo;
    private final String m_textColumnName;
    private final String m_sourceLangCode;
    private final String m_targetLangCode;
    private final DataTableSpec m_outputSpec;

    TranslateOperation(final ConnectionInformation cxnInfo, final String textColumnName, final String sourceLangCode, final String targetLangCode, final DataTableSpec outputSpec) {
        this.m_cxnInfo = cxnInfo;
        this.m_textColumnName = textColumnName;
        this.m_sourceLangCode = sourceLangCode;
        this.m_targetLangCode = targetLangCode;
        this.m_outputSpec = outputSpec;
    }

    BufferedDataTable compute(final ExecutionContext exec, final BufferedDataTable data) throws CanceledExecutionException, InterruptedException, InvalidSettingsException {

        final BufferedDataContainer dc = exec.createDataContainer(m_outputSpec);
        if (data.size() == 0) {
            dc.close();
            return dc.getTable();
        }

        // Create stream enabled input and output ports wrapping the input data table and output table.
        DataTableRowInput in = new DataTableRowInput(data);
        BufferedDataTableRowOutput out = new BufferedDataTableRowOutput(dc);

        // Invoke computation on the stream enabled ports
        try {
            compute(in, out, exec, in.getRowCount());
        }
        finally {
            in.close();
            out.close();
        }

        return out.getDataTable();
    }

    void compute(final RowInput in, final RowOutput out, final ExecutionContext exec, final long rowCount) throws CanceledExecutionException, InterruptedException {

        // Build credentials and provider using account info from the connection object
        // User -> access key ID, password -> secret key
        BasicAWSCredentials creds = new BasicAWSCredentials(m_cxnInfo.getUser(), m_cxnInfo.getPassword());
        AWSCredentialsProvider credProvider = new AWSStaticCredentialsProvider(creds);

        // Create a connection to the Translate service in the provided region
        AmazonTranslate translate = AmazonTranslateClient.builder()
                .withCredentials(credProvider)
                .withRegion(m_cxnInfo.getHost())
                .build();

        int textColumnIdx = in.getDataTableSpec().findColumnIndex(m_textColumnName);
        long inputRowIndex = 0;
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Translate
        // and push the input plus the translation to the output.
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {
            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double) rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }

            // Grab the text to evaluate
            String textValue = null;
            DataCell cell = inputRow.getCell(textColumnIdx);

            if (cell instanceof DocumentValue) {
                textValue = ((DocumentValue) cell).getDocument().getDocumentBodyText();
            }
            else {
                textValue = cell.toString();
            }

            TranslateTextRequest request = new TranslateTextRequest()
                    .withText(textValue)
                    .withSourceLanguageCode(m_sourceLangCode)
                    .withTargetLanguageCode(m_targetLangCode);

            TranslateTextResult result  = translate.translateText(request);

            // TODO just copy the input row key to the output row key
            RowKey key = new RowKey("Row " + inputRowIndex);

            // Create cells containing the output data
            int numCols = m_outputSpec.getNumColumns();
            DataCell[] cells = new DataCell[numCols];
            for (int i = 0; i < numCols - 1; i++) {
                cells[i] = inputRow.getCell(i);
            }
            cells[numCols-1] = new StringCell(result.getTranslatedText());

            // Create a new data row and push it to the output container.
            DataRow row = new DefaultRow(key, cells);
            out.push(row);
            ++inputRowIndex;
        }

    }


}
