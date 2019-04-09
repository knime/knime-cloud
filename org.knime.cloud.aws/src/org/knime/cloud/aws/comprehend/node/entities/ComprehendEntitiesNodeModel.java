package org.knime.cloud.aws.comprehend.node.entities;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.comprehend.ComprehendUtils;
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
 * Node model for node that extracts entities from text using the Amazon Comprehend service.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendEntitiesNodeModel extends NodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(ComprehendEntitiesNodeModel.class);

    // Settings name for the input column name with text to analyze
	static final String CFGKEY_COLUMN_NAME = "TextColumnName";

	// Settings name for the input column name with text to analyze
    static final String CFGKEY_SOURCE_LANG = "SourceLanguage";

    private final SettingsModelString textColumnName =
            new SettingsModelString(
                ComprehendEntitiesNodeModel.CFGKEY_COLUMN_NAME,
                "text");

    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                ComprehendEntitiesNodeModel.CFGKEY_SOURCE_LANG,
                "English");

    // Connection info passed in via the first input port
    private ConnectionInformation cxnInfo;

    /**
     * Constructor for the node model.
     */
    protected ComprehendEntitiesNodeModel() {
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
        final EntityOperation entityOp =
                new EntityOperation(
                    cxnInfo,
                    textColumnName.getStringValue(),
                    sourceLanguage.getStringValue());

        // Access the input data table
        BufferedDataTable table = (BufferedDataTable) inObjects[1];

        // Run the operation over the entire input.
        BufferedDataTable[] result = new BufferedDataTable[] { entityOp.compute(exec, table) };
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo, final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final EntityOperation entityOp =
                new EntityOperation(
                    cxnInfo,
                    textColumnName.getStringValue(),
                    sourceLanguage.getStringValue());

        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                RowInput input = (RowInput)inputs[1];
                RowOutput output = (RowOutput)outputs[0];
                entityOp.compute(input, output, exec, 0L);
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

        DataTableSpec outputSpec = EntityOperation.createDataTableSpec(textColumnName.getStringValue());

        return new DataTableSpec[] {outputSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

        textColumnName.saveSettingsTo(settings);
        sourceLanguage.saveSettingsTo(settings);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

        textColumnName.loadSettingsFrom(settings);
        sourceLanguage.loadSettingsFrom(settings);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        textColumnName.validateSettings(settings);
        sourceLanguage.validateSettings(settings);

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

