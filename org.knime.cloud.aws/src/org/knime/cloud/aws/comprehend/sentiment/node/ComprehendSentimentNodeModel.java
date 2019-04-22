package org.knime.cloud.aws.comprehend.sentiment.node;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * Implementation of the Amazon Comprehend (Sentiment) node.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeModel extends BaseComprehendNodeModel {

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

}

