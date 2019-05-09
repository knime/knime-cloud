package org.knime.cloud.aws.nodes.comprehend.language;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;


/**
 * Implementation of the Amazon Comprehend (Langauge) node.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendLanguageNodeModel extends BaseComprehendNodeModel {

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new LanguageOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.outputTableSpec);
    }

    @Override
    public DataTableSpec generateOutputTableSpec(final DataTableSpec inputSpec) {
        // Define the new columns
        DataColumnSpec[] allColSpecs = new DataColumnSpec[3];
        allColSpecs[0] = new DataColumnSpecCreator("Language", StringCell.TYPE).createSpec();
        allColSpecs[1] = new DataColumnSpecCreator("Language Code", StringCell.TYPE).createSpec();
        allColSpecs[2] = new DataColumnSpecCreator("Confidence", DoubleCell.TYPE).createSpec();

        // Input spec + new columns
        return new DataTableSpec(inputSpec, new DataTableSpec(allColSpecs));
    }

}

