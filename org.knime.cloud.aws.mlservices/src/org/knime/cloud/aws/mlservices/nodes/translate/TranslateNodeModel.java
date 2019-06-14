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
package org.knime.cloud.aws.mlservices.nodes.translate;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.aws.util.AmazonConnectionInformationPortObject;
import org.knime.cloud.aws.util.ConnectionUtils;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.StringValue;
import org.knime.core.data.def.StringCell;
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
import org.knime.ext.textprocessing.util.ColumnSelectionVerifier;

import com.amazonaws.services.translate.AmazonTranslate;

/**
 * Use the Amazon Translate service to translate from a source to a target language.
 *
 * @author Jim Falgout, KNIME AG, Zurich, Switzerland
 */
class TranslateNodeModel extends NodeModel {

    /** The logger instance */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(TranslateNodeModel.class);

    /** Settings name for the input column name with text to analyze */
    private static final String CFG_KEY_COLUMN_NAME = "text_column_name";

    /** Settings name for the source language */
    private static final String CFG_KEY_SOURCE_LANG = "source_language";

    /** Settings name for the target language */
    private static final String CFG_KEY_TARGET_LANG = "target_language";

    /** Default value for the column name */
    private static final String DEF_COL_NAME = "";

    /** Default value for the source language */
    private static final String DEF_SOURCE_LANGUAGE = "Auto-detect";

    /** Default value for the target language */
    private static final String DEF_TARGET_LANGUAGE = "English";

    /** Index of the connection information port */
    private static final int CNX_PORT_IDX = 0;

    /** Index of the data port */
    private static final int DATA_PORT_IDX = 1;

    /**
     * Method to create a {@link SettingsModelString} storing the name of the text column containing the text to
     * translate.
     */
    static final SettingsModelString getTextColModel() {
        return new SettingsModelString(CFG_KEY_COLUMN_NAME, DEF_COL_NAME);
    }

    /** Method to create a {@link SettingsModelString} storing the source language of the input text */
    static final SettingsModelString getSourceLanguageModel() {
        return new SettingsModelString(CFG_KEY_SOURCE_LANG, DEF_SOURCE_LANGUAGE);
    }

    /** Method to create a {@link SettingsModelString} storing the target language to translate the text to */
    static final SettingsModelString getTargetLanguageModel() {
        return new SettingsModelString(CFG_KEY_TARGET_LANG, DEF_TARGET_LANGUAGE);
    }

    /** {@link SettingsModelString} storing the name of the text column containing the text to translate */
    private final SettingsModelString m_textColumnName = getTextColModel();

    /** {@link SettingsModelString} storing the source language of the input text */
    private final SettingsModelString m_sourceLanguage = getSourceLanguageModel();

    /** {@link SettingsModelString} storing the target language to translate the text to */
    private final SettingsModelString m_targetLanguage = getTargetLanguageModel();

    /** Map of supported language and their language code/identifier */
    private static final Map<String, String> SUPPORTED_LANGS;
    static {
        Map<String, String> langMap = new LinkedHashMap<>();
        langMap.put("Arabic", "ar");
        langMap.put("Chinese (Simplified)", "zh");
        langMap.put("Chinese (Traditional)", "zh-TW");
        langMap.put("Czech", "cs");
        langMap.put("Danish", "da");
        langMap.put("Dutch", "nl");
        langMap.put(DEF_TARGET_LANGUAGE, "en");
        langMap.put("Finnish", "fi");
        langMap.put("French", "fr");
        langMap.put("German", "de");
        langMap.put("Hebrew", "he");
        langMap.put("Hindi", "hi");
        langMap.put("Indonesian", "id");
        langMap.put("Italian", "it");
        langMap.put("Japanese", "ja");
        langMap.put("Korean", "ko");
        langMap.put("Malay", "ms");
        langMap.put("Norwegian", "no");
        langMap.put("Persian", "fa");
        langMap.put("Polish", "pl");
        langMap.put("Portuguese", "pt");
        langMap.put("Russian", "ru");
        langMap.put("Spanish", "es");
        langMap.put("Swedish", "sv");
        langMap.put("Turkish", "tr");
        SUPPORTED_LANGS = Collections.unmodifiableMap(langMap);
    }

    /** Map of supported source language */
    static final Map<String, String> SOURCE_LANGS;
    static {
        Map<String, String> srcLangs = new LinkedHashMap<>();
        srcLangs.put(DEF_SOURCE_LANGUAGE, "auto");
        srcLangs.putAll(SUPPORTED_LANGS);
        SOURCE_LANGS = Collections.unmodifiableMap(srcLangs);
    }

    /** Map of supported target languages */
    static final Map<String, String> TARGET_LANGS = SUPPORTED_LANGS;

    /** Constructor for the node model */
    TranslateNodeModel() {
        // Inputs: connection info, data
        // Outputs: data
        super(new PortType[]{AmazonConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[]{BufferedDataTable.TYPE});
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {

        if (inObjects == null || inObjects.length != 2) {
            throw new InvalidSettingsException("Invalid input data. Expected two inputs.");
        }
        final CloudConnectionInformation cxnInfo =
            ((AmazonConnectionInformationPortObject)inObjects[CNX_PORT_IDX]).getConnectionInformation();
        LOGGER.info("Using region: " + cxnInfo.getHost());

        // Access the input data table
        final BufferedDataTable table = (BufferedDataTable)inObjects[DATA_PORT_IDX];

        // Create computation object for the entity operation.
        final TranslateOperation translateOp = new TranslateOperation(cxnInfo, m_textColumnName.getStringValue(),
            SOURCE_LANGS.getOrDefault(m_sourceLanguage.getStringValue(), "auto"),
            TARGET_LANGS.getOrDefault(m_targetLanguage.getStringValue(), "en"),
            createNewDataTableSpec(table.getDataTableSpec()));

        // Run the operation over the entire input.
        return new BufferedDataTable[]{translateOp.compute(exec, table)};
    }

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

            if (!ConnectionUtils.regionSupported(cxnInfo.getHost(), AmazonTranslate.ENDPOINT_PREFIX)) {
                throw new InvalidSettingsException(
                    "Unsupported region for the Amazon Translate service: " + cxnInfo.getHost());
            }
        } else {
            throw new InvalidSettingsException("No connection information available");
        }

        final DataTableSpec tblSpec = (DataTableSpec)inSpecs[DATA_PORT_IDX];
        checkDataTableSpec(tblSpec);

        return new DataTableSpec[]{createNewDataTableSpec(tblSpec)};
    }

    /**
     * Creates the new data table spec based on the input data table spec.
     *
     * @param inputSpec The input data table spec
     * @return Output data table spec
     */
    private final DataTableSpec createNewDataTableSpec(final DataTableSpec inputSpec) {
        final DataColumnSpecCreator colSpec =
            new DataColumnSpecCreator(m_textColumnName.getStringValue() + " (Translated)", StringCell.TYPE);
        return new DataTableSpec(inputSpec, new DataTableSpec(colSpec.createSpec()));
    }

    /**
     * Checks whether the input {@link DataTableSpec} is valid or not.
     *
     * @throws InvalidSettingsException Thrown if data table spec is invalid.
     */
    private void checkDataTableSpec(final DataTableSpec spec) throws InvalidSettingsException {
        final long numOfValidCols = spec.stream()//
            .filter(colSpec -> colSpec.getType().isCompatible(StringValue.class))//
            .count();
        if (numOfValidCols < 1) {
            throw new InvalidSettingsException("There has to be at least one column containing String data!");
        }
        ColumnSelectionVerifier.verifyColumn(m_textColumnName, spec, StringValue.class, null)
            .ifPresent(warningMsg -> setWarningMessage(warningMsg));
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final DataTableSpec dataSpec = (DataTableSpec)inSpecs[DATA_PORT_IDX];
        final ConnectionInformationPortObjectSpec cnxSpec = (ConnectionInformationPortObjectSpec)inSpecs[CNX_PORT_IDX];
        final CloudConnectionInformation cxnInfo = (CloudConnectionInformation)cnxSpec.getConnectionInformation();
        final TranslateOperation translateOp = new TranslateOperation(cxnInfo, m_textColumnName.getStringValue(),
            SOURCE_LANGS.getOrDefault(m_sourceLanguage.getStringValue(), "auto"),
            TARGET_LANGS.getOrDefault(m_targetLanguage.getStringValue(), "en"), createNewDataTableSpec(dataSpec));

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

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.NONDISTRIBUTED_NONSTREAMABLE, InputPortRole.DISTRIBUTED_STREAMABLE};
    }

    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_textColumnName.saveSettingsTo(settings);
        m_sourceLanguage.saveSettingsTo(settings);
        m_targetLanguage.saveSettingsTo(settings);

    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_textColumnName.loadSettingsFrom(settings);
        m_sourceLanguage.loadSettingsFrom(settings);
        m_targetLanguage.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_textColumnName.validateSettings(settings);
        m_sourceLanguage.validateSettings(settings);
        m_targetLanguage.validateSettings(settings);
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
