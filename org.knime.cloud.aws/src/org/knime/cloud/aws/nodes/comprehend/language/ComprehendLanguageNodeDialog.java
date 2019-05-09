package org.knime.cloud.aws.nodes.comprehend.language;

import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.data.StringValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendLanguageNodeDialog extends DefaultNodeSettingsPane {

    /**
     * Configuration dialog for language node.
     */
    @SuppressWarnings("unchecked")
    protected ComprehendLanguageNodeDialog() {
        super();

        addDialogComponent(
            new DialogComponentColumnNameSelection(
                new SettingsModelString(ComprehendUtils.CFG_KEY_DOCUMENT_COL, "text"),
                "Text column to analyze:",
                1,
                StringValue.class)
        );

    }
}

