package org.knime.cloud.aws.nodes.translate;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
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
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * Use the Amazon Translate service to translate from a source to a target language.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class TranslateNodeModel extends NodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(TranslateNodeModel.class);

    // Settings name for the input column name with text to analyze
	static final String CFGKEY_COLUMN_NAME = "TextColumnName";

	// Settings name for the input column name with text to analyze
    static final String CFGKEY_SOURCE_LANG = "SourceLanguage";

    // Settings name for the input column name with text to analyze
    static final String CFGKEY_TARGET_LANG = "TargetLanguage";

    private final SettingsModelString textColumnName =
            new SettingsModelString(
                TranslateNodeModel.CFGKEY_COLUMN_NAME,
                "text");

    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                TranslateNodeModel.CFGKEY_SOURCE_LANG,
                "Auto-detect");

    private final SettingsModelString targetLanguage =
            new SettingsModelString(
                TranslateNodeModel.CFGKEY_TARGET_LANG,
                "English");

    // Connection info passed in via the first input port
    private ConnectionInformation cxnInfo;

    private DataTableSpec outputPortSpec;

    private static Map<String, String> SUPPORTED_LANGS;
    static {
        Map<String, String> langMap = new HashMap<>();
        langMap.put("Arabic", "ar");
        langMap.put("Chinese (Simplified)", "zh");
        langMap.put("Chinese (Traditional)", "zh-TW");
        langMap.put("Czech", "cs");
        langMap.put("Danish", "da");
        langMap.put("Dutch", "nl");
        langMap.put("English", "en");
        langMap.put("Finnish", "fi");
        langMap.put("French", "fr");
        langMap.put("German", "de");
        langMap.put("Hebrew", "he");
        langMap.put("Indonesian", "id");
        langMap.put("Italian", "it");
        langMap.put("Japanese", "ja");
        langMap.put("Korean", "ko");
        langMap.put("Polish", "pl");
        langMap.put("Portuguese", "pt");
        langMap.put("Russian", "ru");
        langMap.put("Swedish", "sv");
        langMap.put("Spanish", "es");
        langMap.put("Turkish", "tr");
        SUPPORTED_LANGS = Collections.unmodifiableMap(langMap);
    }

    static Map<String, String> SOURCE_LANGS;
    static {
        Map<String, String> srcLangs = new HashMap<>();
        srcLangs.put("Auto-detect", "auto");
        srcLangs.putAll(SUPPORTED_LANGS);
        SOURCE_LANGS = Collections.unmodifiableMap(srcLangs);
    }

    static Map<String, String> TARGET_LANGS = SUPPORTED_LANGS;

    /**
     * Constructor for the node model.
     */
    protected TranslateNodeModel() {

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
        final TranslateOperation translateOp =
                new TranslateOperation(
                    cxnInfo,
                    textColumnName.getStringValue(),
                    SOURCE_LANGS.getOrDefault(sourceLanguage.getStringValue(), "auto"),
                    TARGET_LANGS.getOrDefault(targetLanguage.getStringValue(), "en"),
                    this.outputPortSpec);

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

        final TranslateOperation translateOp =
                new TranslateOperation(
                    cxnInfo,
                    textColumnName.getStringValue(),
                    SOURCE_LANGS.getOrDefault(sourceLanguage.getStringValue(), "auto"),
                    TARGET_LANGS.getOrDefault(targetLanguage.getStringValue(), "en"),
                    this.outputPortSpec);

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
        if (!tblSpec.containsName(textColumnName.getStringValue())) {
            throw new InvalidSettingsException("Input column '" + textColumnName.getStringValue() + "' doesn't exit");
        }

        DataTableSpec newColsSpec = new DataTableSpec(new String[] { textColumnName.getStringValue() + " (Translated)" }, new DataType[] { DocumentCell.TYPE });
        this.outputPortSpec =  new DataTableSpec(tblSpec, newColsSpec);

        return new DataTableSpec[] { this.outputPortSpec };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

        textColumnName.saveSettingsTo(settings);
        sourceLanguage.saveSettingsTo(settings);
        targetLanguage.saveSettingsTo(settings);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

        textColumnName.loadSettingsFrom(settings);
        sourceLanguage.loadSettingsFrom(settings);
        targetLanguage.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        textColumnName.validateSettings(settings);
        sourceLanguage.validateSettings(settings);
        targetLanguage.validateSettings(settings);
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

