package org.knime.cloud.aws.comprehend.keyphrases.node;

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
public class ComprehendKeyPhrasesNodeDialog extends DefaultNodeSettingsPane {

    /**
     * Create a basic dialog with column selection and selection for the source language.
     */
    @SuppressWarnings("unchecked")
    protected ComprehendKeyPhrasesNodeDialog() {
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

