package org.knime.cloud.aws.comprehend.syntax.node;

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
import org.knime.ext.textprocessing.data.TermCell2;


/**
 * Node model for the Comprehend Syntax service.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSyntaxNodeModel extends BaseComprehendNodeModel {

    /** The source language of the input text data. */
    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                BaseComprehendNodeModel.CFGKEY_SOURCE_LANG,
                "English");

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new SyntaxOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue(),
            this.outputTableSpec);
    }

    @Override
    public DataTableSpec generateOutputTableSpec(final DataTableSpec inputSpec) {
        DataColumnSpec[] allColSpecs = new DataColumnSpec[9];
        allColSpecs[0] = new DataColumnSpecCreator(textColumnName.getStringValue() + " (Processed)", DocumentCell.TYPE).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Terms", TermCell2.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Token Text", StringCell.TYPE).createSpec();
        allColSpecs[3] = new DataColumnSpecCreator("Token ID", IntCell.TYPE).createSpec();
        allColSpecs[4] = new DataColumnSpecCreator("Part of Speech Code", StringCell.TYPE).createSpec();
        allColSpecs[5] = new DataColumnSpecCreator("Part of Speech", StringCell.TYPE).createSpec();
        allColSpecs[6] = new DataColumnSpecCreator("Confidence", DoubleCell.TYPE).createSpec();
        allColSpecs[7] = new DataColumnSpecCreator("Begin Offset", IntCell.TYPE).createSpec();
        allColSpecs[8] = new DataColumnSpecCreator("End Offset", IntCell.TYPE).createSpec();

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

