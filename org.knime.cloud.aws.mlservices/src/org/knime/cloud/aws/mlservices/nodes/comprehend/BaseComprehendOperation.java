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
package org.knime.cloud.aws.mlservices.nodes.comprehend;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.BufferedDataTableRowOutput;
import org.knime.core.node.streamable.DataTableRowInput;

import com.amazonaws.services.comprehend.AmazonComprehend;

/**
 * Base implementation of a streaming operation for Comprehend nodes.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
public abstract class BaseComprehendOperation implements ComprehendOperation {

    /** AWS connection information. */
    private final CloudConnectionInformation m_cxnInfo;

    /** Name of the input text column to analyze. */
    private final String m_textColumnName;

    /** The output table specification. */
    private final DataTableSpec m_outputTableSpec;

    /**
     * Create a new operation instance.
     *
     * @param cxnInfo AWS connection information
     * @param textColumnName Name of the input text column.
     * @param outputTableSpec specification of the output table of the node
     */
    public BaseComprehendOperation(final CloudConnectionInformation cxnInfo, final String textColumnName,
        final DataTableSpec outputTableSpec) {
        this.m_cxnInfo = cxnInfo;
        this.m_textColumnName = textColumnName;
        this.m_outputTableSpec = outputTableSpec;
    }

    @Override
    public BufferedDataTable compute(final ExecutionContext exec, final BufferedDataTable data)
        throws Exception {
        // Create the data container for the output data
        final BufferedDataContainer dc = exec.createDataContainer(m_outputTableSpec);

        // If not input data is available in the input, then return an empty output
        // table.
        if (data.size() == 0) {
            dc.close();
            return dc.getTable();
        }

        // Create stream enabled input and output ports wrapping the input data table
        // and output table.
        final DataTableRowInput in = new DataTableRowInput(data);
        final BufferedDataTableRowOutput out = new BufferedDataTableRowOutput(dc);
        // Create a connection to the Comprehend service in the provided region
        final ComprehendConnection connection = new ComprehendConnection(m_cxnInfo);
        final AmazonComprehend comprehendClient = connection.getClient();
        // Access the input data table
        final int textColumnIdx = in.getDataTableSpec().findColumnIndex(m_textColumnName);

        // Invoke computation on the stream enabled ports
        try {
            compute(in, out, comprehendClient, textColumnIdx, exec, in.getRowCount());
        } finally {
            in.close();
            out.close();
        }

        return out.getDataTable();
    }
}
