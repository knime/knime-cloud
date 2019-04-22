package org.knime.cloud.aws.comprehend.sentiment.node;

import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.data.DocumentValue;

/**
 *
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendSentimentNodeDialog extends DefaultNodeSettingsPane {

    /**
     * New pane for configuring MyExampleNode node dialog.
     * This is just a suggestion to demonstrate possible default dialog
     * components.
     */
    @SuppressWarnings("unchecked")
    protected ComprehendSentimentNodeDialog() {
        super();

        addDialogComponent(
            new DialogComponentColumnNameSelection(
                new SettingsModelString(ComprehendUtils.CFGKEY_COLUMN_NAME, "text"),
                "Text column to analyze:",
                1,
                DocumentValue.class)
        );

        addDialogComponent(
            new DialogComponentStringSelection(
                new SettingsModelString(ComprehendUtils.CFGKEY_SOURCE_LANG, "English"),
                "Source language:",
                ComprehendUtils.LANG_MAP.keySet()
                )
            );

    }
}

