package org.knime.cloud.aws.comprehend.syntax.node;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.ext.textprocessing.data.DocumentCell;


/**
 * This is the model implementation of MyExampleNode.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSyntaxNodeModel extends BaseComprehendNodeModel {


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
        // Repeat the input text column adding in 7 columns of data returned from the AWS call.
        DataColumnSpec[] allColSpecs = new DataColumnSpec[8];
        allColSpecs[0] = new DataColumnSpecCreator(textColumnName.getStringValue() + " (Processed)", DocumentCell.TYPE).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Token Text", StringCell.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Token ID", IntCell.TYPE).createSpec();
        allColSpecs[3] = new DataColumnSpecCreator("Part of Speech Code", StringCell.TYPE).createSpec();
        allColSpecs[4] = new DataColumnSpecCreator("Part of Speech", StringCell.TYPE).createSpec();
        allColSpecs[5] = new DataColumnSpecCreator("Confidence", DoubleCell.TYPE).createSpec();
        allColSpecs[6] = new DataColumnSpecCreator("Begin Offset", IntCell.TYPE).createSpec();
        allColSpecs[7] = new DataColumnSpecCreator("End Offset", IntCell.TYPE).createSpec();

        return new DataTableSpec(inputSpec, new DataTableSpec(allColSpecs));
    }

}

