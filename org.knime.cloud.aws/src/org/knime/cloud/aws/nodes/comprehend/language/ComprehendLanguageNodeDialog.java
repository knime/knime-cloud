package org.knime.cloud.aws.nodes.comprehend.language;

import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.data.DocumentValue;

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
                new SettingsModelString(ComprehendUtils.CFGKEY_COLUMN_NAME, "text"),
                "Text column to analyze:",
                1,
                DocumentValue.class)
        );

    }
}

