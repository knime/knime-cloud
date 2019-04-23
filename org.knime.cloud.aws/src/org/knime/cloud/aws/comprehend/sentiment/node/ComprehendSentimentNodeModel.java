package org.knime.cloud.aws.comprehend.sentiment.node;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * Implementation of the Amazon Comprehend (Sentiment) node.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeModel extends BaseComprehendNodeModel {

    /** The source language of the input text data. */
    private final SettingsModelString sourceLanguage =
            new SettingsModelString(
                BaseComprehendNodeModel.CFGKEY_SOURCE_LANG,
                "English");

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new SentimentOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue(),
            this.outputTableSpec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec generateOutputTableSpec(final DataTableSpec inputTableSpec) {
        DataColumnSpec[] allColSpecs = new DataColumnSpec[6];
        allColSpecs[0] = new DataColumnSpecCreator(textColumnName.getStringValue() + " (Processed)", DocumentCell.TYPE).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Sentiment", StringCell.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Score (mixed)", DoubleCell.TYPE).createSpec();
        allColSpecs[3] = new DataColumnSpecCreator("Score (positive)", DoubleCell.TYPE).createSpec();
        allColSpecs[4] = new DataColumnSpecCreator("Score (neutral)", DoubleCell.TYPE).createSpec();
        allColSpecs[5] = new DataColumnSpecCreator("Score (negative)", DoubleCell.TYPE).createSpec();

        return new DataTableSpec(inputTableSpec, new DataTableSpec(allColSpecs));
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

