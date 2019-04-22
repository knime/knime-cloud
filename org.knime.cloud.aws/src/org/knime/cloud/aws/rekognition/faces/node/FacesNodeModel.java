package org.knime.cloud.aws.rekognition.faces.node;

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
 * Use the Amazon Translate service to translate from a source to a target language.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class FacesNodeModel extends NodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(FacesNodeModel.class);

    // Settings name for the input column name with text to analyze
	static final String CFGKEY_COLUMN_NAME = "ImageColumnName";

    private final SettingsModelString imageColumnName =
            new SettingsModelString(
                FacesNodeModel.CFGKEY_COLUMN_NAME,
                "image");


    // Connection info passed in via the first input port
    private ConnectionInformation cxnInfo;

    /**
     * Constructor for the node model.
     */
    protected FacesNodeModel() {

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

        // Create computation object for the entity operation.
        final FacesOperation translateOp =
                new FacesOperation(
                    cxnInfo,
                    imageColumnName.getStringValue());

        // Access the input data table
        BufferedDataTable table = (BufferedDataTable) inObjects[1];

        // Run the operation over the entire input.
        BufferedDataTable[] result = new BufferedDataTable[] { translateOp.compute(exec, table) };
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final FacesOperation translateOp =
                new FacesOperation(
                    cxnInfo,
                    imageColumnName.getStringValue());

        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                RowInput input = (RowInput)inputs[1];
                RowOutput output = (RowOutput)outputs[0];
                translateOp.compute(input, output, exec, 0L);
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
        }
        else {
            throw new InvalidSettingsException("No connection information available");
        }

        DataTableSpec tblSpec = (DataTableSpec) inSpecs[1];
        if (!tblSpec.containsName(imageColumnName.getStringValue())) {
            throw new InvalidSettingsException("Input column '" + imageColumnName.getStringValue() + "' doesn't exit");
        }

        DataTableSpec outputSpec = FacesOperation.createDataTableSpec(imageColumnName.getStringValue());

        return new DataTableSpec[] {outputSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

        imageColumnName.saveSettingsTo(settings);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

        imageColumnName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        imageColumnName.validateSettings(settings);
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

}

