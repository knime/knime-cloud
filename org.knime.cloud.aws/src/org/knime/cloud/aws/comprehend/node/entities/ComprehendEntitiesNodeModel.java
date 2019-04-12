package org.knime.cloud.aws.comprehend.node.entities;

import org.knime.cloud.aws.comprehend.BaseComprehendNodeModel;
import org.knime.cloud.aws.comprehend.ComprehendOperation;
import org.knime.core.node.NodeLogger;


/**
 * Node model for node that extracts entities from text using the Amazon Comprehend service.
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendEntitiesNodeModel extends BaseComprehendNodeModel {

    // the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(ComprehendEntitiesNodeModel.class);

    @Override
    protected ComprehendOperation getOperationInstance() {
        return new EntityOperation(
            this.cxnInfo,
            this.textColumnName.getStringValue(),
            this.sourceLanguage.getStringValue());
    }
}

