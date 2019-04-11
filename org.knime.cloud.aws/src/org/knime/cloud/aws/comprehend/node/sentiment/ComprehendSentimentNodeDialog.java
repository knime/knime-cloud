package org.knime.cloud.aws.comprehend.node.sentiment;

import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.data.StringValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

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
                new SettingsModelString(ComprehendSentimentNodeModel.CFGKEY_COLUMN_NAME, "text"),
                "Text column to analyze:",
                1,
                StringValue.class)
        );

        addDialogComponent(
            new DialogComponentStringSelection(
                new SettingsModelString(ComprehendSentimentNodeModel.CFGKEY_SOURCE_LANG, "English"),
                "Source language:",
                ComprehendUtils.LANG_MAP.keySet()
                )
            );

    }
}

