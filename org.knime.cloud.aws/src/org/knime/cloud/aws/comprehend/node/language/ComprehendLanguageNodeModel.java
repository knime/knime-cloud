package org.knime.cloud.aws.comprehend.node.language;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.node.NodeLogger;


/**
 * Implementation of the Amazon Comprehend (Sentiment) node.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendLanguageNodeModel extends BaseComprehendNodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(ComprehendLanguageNodeModel.class);

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new LanguageOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue());
    }

}

