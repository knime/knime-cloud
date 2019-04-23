package org.knime.cloud.aws.comprehend.entities.node;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * Node model for node that extracts entities from text using the Amazon Comprehend service.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendEntitiesNodeModel extends BaseComprehendNodeModel {

    /** The source language of the input text data. */
    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                BaseComprehendNodeModel.CFGKEY_SOURCE_LANG,
                "English");

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new EntityOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue(),
            this.outputTableSpec);
    }

    @Override
    public DataTableSpec generateOutputTableSpec(final DataTableSpec inputSpec) {

        // The columns added from the AWS call.
        DataColumnSpec[] allColSpecs = new DataColumnSpec[5];
        allColSpecs[0] = new DataColumnSpecCreator(textColumnName.getStringValue() + " (Processed)", DocumentCell.TYPE).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Entity", StringCell.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Confidence", DoubleCell.TYPE).createSpec();
        allColSpecs[3] = new DataColumnSpecCreator("Begin Offset", IntCell.TYPE).createSpec();
        allColSpecs[4] = new DataColumnSpecCreator("End Offset", IntCell.TYPE).createSpec();

        // Along with the input data columns.
        return new DataTableSpec(inputSpec, new DataTableSpec(allColSpecs));
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
}

