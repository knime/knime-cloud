package org.knime.cloud.aws.comprehend.node.sentiment;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.node.NodeLogger;


/**
 * Implementation of the Amazon Comprehend (Sentiment) node.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeModel extends BaseComprehendNodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(ComprehendSentimentNodeModel.class);

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new SentimentOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue());
    }

}

