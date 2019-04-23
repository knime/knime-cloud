package org.knime.cloud.aws.comprehend;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.core.data.DataTableSpec;
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
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;


/**
 * Base node model for all Amazon Comprehend nodes. Captures all the commonality between the implementations.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public abstract class BaseComprehendNodeModel extends NodeModel {

    /** Logger instance */
    protected static final NodeLogger logger = NodeLogger
            .getLogger(BaseComprehendNodeModel.class);

    /** Settings name for the input column name with text to analyze */
	protected static final String CFGKEY_COLUMN_NAME = "TextColumnName";

	/** Settings name for the input column name with the source language value (optioal) */
    protected static final String CFGKEY_SOURCE_LANG = "SourceLanguage";

    /** Name of the input text column to analyze */
    protected final SettingsModelString textColumnName =
            new SettingsModelString(
                BaseComprehendNodeModel.CFGKEY_COLUMN_NAME,
                "text");

    /** Connection info passed in via the first input port */
    protected ConnectionInformation cxnInfo;

    /** Output data table specification. */
    protected DataTableSpec outputTableSpec;

    /**
     * Constructor for the node model.
     */
    protected BaseComprehendNodeModel() {

        // Inputs: connection info, data
        // Outputs: data

        super(
            new PortType[] { ConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[] { BufferedDataTable.TYPE });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        logger.info("Using region: " + cxnInfo.getHost());
        if (inObjects == null || inObjects.length != 2) {
            throw new InvalidSettingsException("Invalid input data. Expected two inputs.");
        }

        // Create computation object for this operation.
        final ComprehendOperation op = getOperationInstance();

        // Access the input data table
        BufferedDataTable table = (BufferedDataTable) inObjects[1];

        // Run the operation over the entire input.
        BufferedDataTable[] result = new BufferedDataTable[] { op.compute(exec, table) };
        return result;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final ComprehendOperation op = getOperationInstance();

        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                RowInput input = (RowInput)inputs[1];
                RowOutput output = (RowOutput)outputs[0];
                op.compute(input, output, exec, 0L);
                input.close();
                output.close();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_NONSTREAMABLE, InputPortRole.DISTRIBUTED_STREAMABLE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        if (inSpecs[0] != null) {
            final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
            cxnInfo = object.getConnectionInformation();
            // Check if the port object has connection information
            if (cxnInfo == null) {
                throw new InvalidSettingsException("No connection information available");
            }

            if (!ComprehendUtils.regionSupported(cxnInfo.getHost())) {
                throw new InvalidSettingsException("Unsupported region for the Amazon Comprehend service: " + cxnInfo.getHost());
            }

        }
        else {
            throw new InvalidSettingsException("No connection information available");
        }

        DataTableSpec tblSpec = (DataTableSpec) inSpecs[1];
        if (!tblSpec.containsName(textColumnName.getStringValue())) {
            throw new InvalidSettingsException("Input column '" + textColumnName.getStringValue() + "' doesn't exit");
        }

        this.outputTableSpec =  generateOutputTableSpec(tblSpec);

        return new DataTableSpec[] { this.outputTableSpec };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

        textColumnName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

        textColumnName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        textColumnName.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException, CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {

    }

    /**
     * Create an instance of the operation execution for this node.
     * @return an instance of the operation executor
     */
    protected abstract ComprehendOperation getOperationInstance();

    /** Generate the output table spec given the input table spec and the name of the text column.
     *
     * @param inputTableSpec input table specification
     * @return the generated output table specification
     */
    protected abstract DataTableSpec generateOutputTableSpec(DataTableSpec inputTableSpec);

}

