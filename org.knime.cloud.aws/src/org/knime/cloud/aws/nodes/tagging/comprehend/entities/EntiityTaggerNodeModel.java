package org.knime.cloud.aws.nodes.tagging.comprehend.entities;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * Tags the input document using the Amazon Comprehend service to detect entities.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class EntiityTaggerNodeModel extends BaseComprehendNodeModel {

    private final SettingsModelString tokenizerName =
            new SettingsModelString(
                ComprehendUtils.CFGKEY_TOKENIZER,
                "OpenNLP English WordTokenizer");

    /** The source language of the input text data. */
    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                BaseComprehendNodeModel.CFGKEY_SOURCE_LANG,
                "English");

    /**
     * Create the entities node model with the data port being first and the connection info second.
     * Using this order against convention to enable reusing the <code>TaggerNodeSettingsPane2</code>
     * as the basis of the node dialog.
     */
    public EntiityTaggerNodeModel() {
        super(
            new PortType[] { BufferedDataTable.TYPE, ConnectionInformationPortObject.TYPE },
            new PortType[] { BufferedDataTable.TYPE });
    }

    @Override
    protected ComprehendOperation getOperationInstance() {

        // Return a new tagger operation
        return new EntityTaggerOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue(),
            this.tokenizerName.getStringValue(),
            this.outputTableSpec);
    }

    @Override
    protected DataTableSpec generateOutputTableSpec(final DataTableSpec inputSpec) {

        // The columns added from the AWS call.
        DataColumnSpec[] allColSpecs = new DataColumnSpec[1];
        allColSpecs[0] = new DataColumnSpecCreator(textColumnName.getStringValue() + " (Processed)", DocumentCell.TYPE).createSpec();

        // Along with the input data columns.
        return new DataTableSpec(inputSpec, new DataTableSpec(allColSpecs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[]{InputPortRole.DISTRIBUTED_STREAMABLE, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

        super.saveSettingsTo(settings);
        sourceLanguage.saveSettingsTo(settings);
        tokenizerName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

        super.loadValidatedSettingsFrom(settings);
        sourceLanguage.loadSettingsFrom(settings);
        tokenizerName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {

        super.validateSettings(settings);
        sourceLanguage.validateSettings(settings);
        tokenizerName.validateSettings(settings);
    }

    @Override
    protected int getCxnPortIndex() {
        return 1;
    }

    @Override
    protected int getDataPortIndex() {
        return 0;
    }
}

