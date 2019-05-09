package org.knime.cloud.aws.nodes.translate;

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
public class TranslateNodeDialog extends DefaultNodeSettingsPane {

    /**
     * New pane for configuring MyExampleNode node dialog.
     * This is just a suggestion to demonstrate possible default dialog
     * components.
     */
    @SuppressWarnings("unchecked")
    protected TranslateNodeDialog() {
        super();

        addDialogComponent(
            new DialogComponentColumnNameSelection(
                new SettingsModelString(TranslateNodeModel.CFGKEY_COLUMN_NAME, "text"),
                "Text column to translate:",
                1,
                DocumentValue.class)
        );

        addDialogComponent(
            new DialogComponentStringSelection(
                new SettingsModelString(TranslateNodeModel.CFGKEY_SOURCE_LANG, "Auto-detect"),
                "Source language:",
                TranslateNodeModel.SOURCE_LANGS.keySet()
                )
            );

        addDialogComponent(
            new DialogComponentStringSelection(
                new SettingsModelString(TranslateNodeModel.CFGKEY_TARGET_LANG, "English"),
                "Target language:",
                TranslateNodeModel.TARGET_LANGS.keySet()
                )
            );

    }
}

