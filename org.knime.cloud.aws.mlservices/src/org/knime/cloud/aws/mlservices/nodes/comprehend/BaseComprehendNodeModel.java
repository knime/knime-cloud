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

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.mlservices.utils.comprehend.ComprehendUtils;
import org.knime.cloud.aws.mlservices.utils.connection.AmazonConnectionInformationPortObject;
import org.knime.cloud.aws.mlservices.utils.connection.ConnectionUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.ext.textprocessing.util.ColumnSelectionVerifier;

import com.amazonaws.services.comprehend.AmazonComprehend;

/**
 * Base node model for all Amazon Comprehend nodes. Captures all the commonality between the implementations.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
public abstract class BaseComprehendNodeModel extends NodeModel {

    /** Logger instance */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(BaseComprehendNodeModel.class);

    /** Connection port. */
    private static final int CNX_PORT_IDX = 0;

    /** Connection port. */
    private static final int DATA_PORT_IDX = 1;

    /** Name of the input text column to analyze */
    private final SettingsModelString m_textColumnName = ComprehendUtils.getTextColumnNameModel();

    /**
     * Constructor for the node model.
     */
    protected BaseComprehendNodeModel() {
        // Inputs: connection info, data
        // Outputs: data
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        if (inObjects == null || inObjects.length != 2) {
            throw new InvalidSettingsException("Invalid input data. Expected two inputs.");
        }
        final CloudConnectionInformation cxnInfo = ((AmazonConnectionInformationPortObject)inObjects[CNX_PORT_IDX])
                .getConnectionInformation();
        LOGGER.info("Using region: " + cxnInfo.getHost());

        // Create computation object for this operation.
        final BufferedDataTable table = (BufferedDataTable)inObjects[DATA_PORT_IDX];
        final DataTableSpec inputTableSpec = table.getDataTableSpec();
        final ComprehendOperation op =
            getOperationInstance(cxnInfo, generateOutputTableSpec(inputTableSpec), m_textColumnName.getStringValue());

        // Run the operation over the entire input.
        return new BufferedDataTable[]{op.compute(exec, table)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        if (inSpecs[CNX_PORT_IDX] != null) {
            final ConnectionInformationPortObjectSpec object =
                (ConnectionInformationPortObjectSpec)inSpecs[CNX_PORT_IDX];
            final ConnectionInformation cxnInfo = object.getConnectionInformation();
            // Check if the port object has connection information
            if (cxnInfo == null) {
                throw new InvalidSettingsException("No connection information available");
            }

            if (!ConnectionUtils.regionSupported(cxnInfo.getHost(), AmazonComprehend.ENDPOINT_PREFIX)) {
                throw new InvalidSettingsException(
                    "Unsupported region for the Amazon Comprehend service: " + cxnInfo.getHost());
            }

        } else {
            throw new InvalidSettingsException("No connection information available");
        }

        final DataTableSpec tblSpec = (DataTableSpec)inSpecs[DATA_PORT_IDX];
        checkDataTableSpec(tblSpec);

        return new DataTableSpec[]{generateOutputTableSpec(tblSpec)};
    }

    /**
     * Checks and verifies the data table spec
     *
     * @param inputSpec The input data table spec
     * @throws InvalidSettingsException Thrown, if the spec is invalid
     */
    protected void checkDataTableSpec(final DataTableSpec inputSpec) throws InvalidSettingsException {
        final long numOfValidCols = inputSpec.stream()//
            .filter(colSpec -> colSpec.getType().isCompatible(StringValue.class))//
            .count();
        if (numOfValidCols < 1) {
            throw new InvalidSettingsException("There has to be at least one column containing String data!");
        }
        ColumnSelectionVerifier.verifyColumn(m_textColumnName, inputSpec, StringValue.class, null)
            .ifPresent(warningMsg -> setWarningMessage(warningMsg));
    }

    /**
     * Create an instance of the operation execution for this node.
     *
     * @param cxnInfo The connection information
     * @param outputSpec The output data table spec
     * @param textColumn The name of the text column to process
     * @return an instance of the operation executor
     */
    protected abstract ComprehendOperation getOperationInstance(final CloudConnectionInformation cxnInfo,
        final DataTableSpec outputSpec, final String textColumn);

    /**
     * Generate the output table spec given the input table spec and the name of the text column.
     *
     * @param inputTableSpec input table specification
     * @return the generated output table specification
     */
    protected abstract DataTableSpec generateOutputTableSpec(DataTableSpec inputTableSpec);

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final DataTableSpec spec = (DataTableSpec)inSpecs[DATA_PORT_IDX];
        final ConnectionInformationPortObjectSpec cnxSpec = (ConnectionInformationPortObjectSpec)inSpecs[CNX_PORT_IDX];
        final int textColIdx = spec.findColumnIndex(m_textColumnName.getStringValue());
        final CloudConnectionInformation cxnInfo = (CloudConnectionInformation)cnxSpec.getConnectionInformation();
        final ComprehendOperation op = getOperationInstance(cxnInfo,
            generateOutputTableSpec(((DataTableSpec)inSpecs[DATA_PORT_IDX])), m_textColumnName.getStringValue());
        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                final RowInput input = (RowInput)inputs[DATA_PORT_IDX];
                final RowOutput output = (RowOutput)outputs[0];
                final ComprehendConnection connection = new ComprehendConnection(cxnInfo);
                final AmazonComprehend comprehendClient = connection.getClient();
                op.compute(input, output, comprehendClient, textColIdx, exec, 0L);
                input.close();
                output.close();
            }
        };
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_NONSTREAMABLE, InputPortRole.DISTRIBUTED_STREAMABLE};
    }

    /**
     * Returns the {@link SettingsModelString} storing the name of the text column to process
     *
     * @return Returns the {@link SettingsModelString} storing the name of the text column to process
     */
    protected SettingsModelString getTextColumnModel() {
        return m_textColumnName;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_textColumnName.saveSettingsTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_textColumnName.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_textColumnName.validateSettings(settings);
    }

    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do here...
    }

    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do here...
    }

    @Override
    protected void reset() {
        // Nothing to do here...
    }
}
