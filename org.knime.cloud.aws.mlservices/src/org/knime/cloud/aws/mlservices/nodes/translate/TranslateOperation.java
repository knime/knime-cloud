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
package org.knime.cloud.aws.mlservices.nodes.translate;

import java.util.stream.Stream;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentValue;

import com.amazonaws.services.translate.AmazonTranslate;
import com.amazonaws.services.translate.model.TranslateTextRequest;
import com.amazonaws.services.translate.model.TranslateTextResult;

/**
 * Class to provide functionality to translate the text of a column from one language to another
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class TranslateOperation {

    /** AWS connection information. */
    private final CloudConnectionInformation m_cxnInfo;

    /** Name of the input text column to analyze. */
    private final String m_textColumnName;

    /** The source language */
    private final String m_sourceLangCode;

    /** The target language */
    private final String m_targetLangCode;

    /** The output table specification. */
    private final DataTableSpec m_outputSpec;

    /**
     * Creates a new {@code TranslateOperation} instance.
     *
     * @param cxnInfo The connection information
     * @param textColumnName The text column name
     * @param sourceLangCode The source language code
     * @param targetLangCode The target language code
     * @param outputSpec The output spec
     */
    TranslateOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final String sourceLangCode, final String targetLangCode, final DataTableSpec outputSpec) {
        this.m_cxnInfo = cxnInfo;
        this.m_textColumnName = textColumnName;
        this.m_sourceLangCode = sourceLangCode;
        this.m_targetLangCode = targetLangCode;
        this.m_outputSpec = outputSpec;
    }

    /**
     * Compute method for a buffered data table. Used when streaming is not enabled.
     *
     * @param exec the execution context
     * @param data the input buffered data table
     * @return the output buffered data table
     * @throws Exception
     */
    BufferedDataTable compute(final ExecutionContext exec, final BufferedDataTable data) throws Exception {
        // Create the data container for the output data
        final BufferedDataContainer dc = exec.createDataContainer(m_outputSpec);
        // If not input data is available in the input, then return an empty output
        // table.
        if (data.size() == 0) {
            dc.close();
            return dc.getTable();
        }

        // Create stream enabled input and output ports wrapping the input data table and output table.
        final DataTableRowInput in = new DataTableRowInput(data);
        final BufferedDataTableRowOutput out = new BufferedDataTableRowOutput(dc);

        // Invoke computation on the stream enabled ports
        try {
            compute(in, out, exec, in.getRowCount());
        } finally {
            in.close();
            out.close();
        }

        return out.getDataTable();
    }

    void compute(final RowInput in, final RowOutput out, final ExecutionContext exec, final long rowCount)
        throws Exception {

        // Create a connection to the Translate service in the provided region
        final TranslateConnection conn = new TranslateConnection(m_cxnInfo);
        final AmazonTranslate translate = conn.getClient();

        int textColumnIdx = in.getDataTableSpec().findColumnIndex(m_textColumnName);
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Translate
        // and push the input plus the translation to the output.
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
            final DataCell cell = inputRow.getCell(textColumnIdx);
            // Create cells containing the output data.
            // Copy the input data to the output
            final int numInputColumns = inputRow.getNumCells();
            DataCell[] cells =
                Stream.generate(DataType::getMissingCell).limit(numInputColumns + 1).toArray(DataCell[]::new);
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
                final TranslateTextRequest request = new TranslateTextRequest().withText(textValue)
                    .withSourceLanguageCode(m_sourceLangCode).withTargetLanguageCode(m_targetLangCode);

                final TranslateTextResult result = translate.translateText(request);
                cells[numInputColumns] = new StringCell(result.getTranslatedText());
            }
            // Create a new data row and push it to the output container.
            out.push(new DefaultRow(inputRow.getKey(), cells));
        }

    }

}
