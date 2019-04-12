package org.knime.cloud.aws.comprehend.node.syntax;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.node.NodeLogger;


/**
 * This is the model implementation of MyExampleNode.
 * This is an example node provided by KNIME.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSyntaxNodeModel extends BaseComprehendNodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(ComprehendSyntaxNodeModel.class);

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new SyntaxOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue());
    }

}

