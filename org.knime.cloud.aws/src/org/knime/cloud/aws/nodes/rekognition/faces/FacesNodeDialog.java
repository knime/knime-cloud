package org.knime.cloud.aws.nodes.rekognition.faces;

import org.knime.core.data.image.ImageValue;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 *
 * @author KNIME AG, Zurich, Switzerland
 */
public class FacesNodeDialog extends DefaultNodeSettingsPane {

    /**
     * New pane for configuring MyExampleNode node dialog.
     * This is just a suggestion to demonstrate possible default dialog
     * components.
     */
    @SuppressWarnings("unchecked")
    protected FacesNodeDialog() {
        super();

        addDialogComponent(
            new DialogComponentColumnNameSelection(
                new SettingsModelString(FacesNodeModel.CFGKEY_COLUMN_NAME, "image"),
                "Image column to analyze:",
                1,
                ImageValue.class)
        );

    }
}

