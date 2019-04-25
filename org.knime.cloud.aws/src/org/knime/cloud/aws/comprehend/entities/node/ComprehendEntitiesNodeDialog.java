package org.knime.cloud.aws.comprehend.entities.node;

import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.textprocessing.nodes.tagging.TaggerNodeSettingsPane2;

/**
 *
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class ComprehendEntitiesNodeDialog extends TaggerNodeSettingsPane2 {

    /**
     * Create a basic dialog with column selection and selection for the source language.
     */
//    @SuppressWarnings("unchecked")
    protected ComprehendEntitiesNodeDialog() {
        super();

//        addDialogComponent(
//            new DialogComponentColumnNameSelection(
//                new SettingsModelString(ComprehendUtils.CFGKEY_COLUMN_NAME, "text"),
//                "Text column to analyze:",
//                1,
//                DocumentValue.class)
//        );

        addDialogComponent(
            new DialogComponentStringSelection(
                new SettingsModelString(ComprehendUtils.CFGKEY_SOURCE_LANG, "English"),
                "Source language:",
                ComprehendUtils.LANG_MAP.keySet()
                )
            );

    }
}

