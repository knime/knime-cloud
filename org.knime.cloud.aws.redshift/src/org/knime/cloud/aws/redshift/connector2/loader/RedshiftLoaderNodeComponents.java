/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector2.loader;

import java.nio.charset.Charset;
import java.util.List;

import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.database.node.io.load.DBLoaderNode2.DialogDelegate;
import org.knime.database.node.io.load.impl.fs.ConnectedCsvLoaderNodeComponents2;

/**
 * Node dialog components and corresponding settings for {@link RedshiftLoaderNode}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftLoaderNodeComponents extends ConnectedCsvLoaderNodeComponents2 {

    private final SettingsModelString m_fileFormatSelectionModel;

    private final DialogComponentButtonGroup m_fileFormatSelectionComponent;

    private final SettingsModelString m_authorizationModel;

    private final DialogComponentString m_authorizationComponent;

    private final SettingsModelString m_compressionModel;

    private final DialogComponentStringSelection m_compressionComponent;

    private final SettingsModelInteger m_chunkSizeModel;

    private final DialogComponentNumber m_chunkSizeComponent;

    private final SettingsModelLong m_fileSizeModel;

    private final DialogComponentNumber m_fileSizeComponent;

    /**
     * Constructs a {@link RedshiftLoaderNodeComponents} object.
     *
     * @param dialogDelegate the delegate of the node dialog to create components for.
     */
    public RedshiftLoaderNodeComponents(final DialogDelegate dialogDelegate) {
        super(dialogDelegate);
        m_fileFormatSelectionModel = RedshiftLoaderNodeSettings.createFileFormatSelectionModel();
        m_fileFormatSelectionComponent = createFileFormatSelectionComponent(m_fileFormatSelectionModel);

        m_authorizationModel = RedshiftLoaderNodeSettings.createAuthorizationModel();
        m_authorizationComponent = createAuthorizationComponent(m_authorizationModel);

        m_compressionModel = RedshiftLoaderNodeSettings.createCompressionModel();
        m_compressionComponent = createCompressionComponent(m_compressionModel);

        m_chunkSizeModel = RedshiftLoaderNodeSettings.createChunkSizeModel();
        m_chunkSizeComponent = createChunkSizeComponent(m_chunkSizeModel);

        m_fileSizeModel = RedshiftLoaderNodeSettings.createFileSizeModel();
        m_fileSizeComponent = createFileSizeComponent(m_fileSizeModel);
    }

    /**
     * Constructs a {@link RedshiftLoaderNodeComponents} object.
     *
     * @param dialogDelegate the delegate of the node dialog to create components for.
     * @param charsets the allowed character set options.
     * @throws NullPointerException if {@code charsets} or any of its elements is {@code null}.
     */
    public RedshiftLoaderNodeComponents(final DialogDelegate dialogDelegate, final List<Charset> charsets) {
        super(dialogDelegate, charsets);
        m_fileFormatSelectionModel = RedshiftLoaderNodeSettings.createFileFormatSelectionModel();
        m_fileFormatSelectionComponent = createFileFormatSelectionComponent(m_fileFormatSelectionModel);

        m_authorizationModel = RedshiftLoaderNodeSettings.createAuthorizationModel();
        m_authorizationComponent = createAuthorizationComponent(m_authorizationModel);

        m_compressionModel = RedshiftLoaderNodeSettings.createCompressionModel();
        m_compressionComponent = createCompressionComponent(m_compressionModel);

        m_chunkSizeModel = RedshiftLoaderNodeSettings.createChunkSizeModel();
        m_chunkSizeComponent = createChunkSizeComponent(m_chunkSizeModel);

        m_fileSizeModel = RedshiftLoaderNodeSettings.createFileSizeModel();
        m_fileSizeComponent = createFileSizeComponent(m_fileSizeModel);
    }

    /**
     * Creates the authorization component.
     *
     * @param authorizationModel the already created authorization settings model.
     * @return a {@link DialogComponentString} object.
     */
    private static DialogComponentString createAuthorizationComponent(final SettingsModelString authorizationModel) {
        final DialogComponentString comp =
            new DialogComponentString(authorizationModel, "Authorization parameters: ", true, 90);
        comp.setToolTipText("Such as IAM_ROLE 'arn:aws:iam::<aws-account-id>:role/<role-name>' or "
            + "ACCESS_KEY_ID '<access-key-id>'  SECRET_ACCESS_KEY '<secret-access-key>' "
            + "SESSION_TOKEN '<temporary-token>';");
        return comp;
    }

    /**
     * Creates the file format selection component.
     *
     * @param fileFormatSelectionModel the already created file format selection settings model.
     * @return a {@link DialogComponentButtonGroup} object.
     */
    private static DialogComponentButtonGroup
        createFileFormatSelectionComponent(final SettingsModelString fileFormatSelectionModel) {
        return new DialogComponentButtonGroup(fileFormatSelectionModel, null, true, RedshiftLoaderFileFormat.values());
    }

    private static DialogComponentStringSelection
        createCompressionComponent(final SettingsModelString compressionModel) {
        return new DialogComponentStringSelection(compressionModel, "Compression method: ", "<NONE>");
    }

    private static DialogComponentNumber createChunkSizeComponent(final SettingsModelInteger chunkSizeModel) {
        return new DialogComponentNumber(chunkSizeModel, "Within file chunk size: ", 1024, 10);
    }

    private static DialogComponentNumber createFileSizeComponent(final SettingsModelLong fileSizeModel) {
        return new DialogComponentNumber(fileSizeModel, "File size: ", 1024, 15);
    }

    /**
     * Gets the file format selection component.
     *
     * @return a {@link DialogComponentButtonGroup} object.
     */
    public DialogComponentButtonGroup getFileFormatSelectionComponent() {
        return m_fileFormatSelectionComponent;
    }

    /**
     * Gets the file format selection settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    public SettingsModelString getFileFormatSelectionModel() {
        return m_fileFormatSelectionModel;
    }

    /**
     * @return the authorizationComponent
     */
    public DialogComponentString getAuthorizationComponent() {
        return m_authorizationComponent;
    }

    /**
     * @return the authorizationModel
     */
    public SettingsModelString getAuthorizationModel() {
        return m_authorizationModel;
    }

    /**
     * @return the compressionComponent
     */
    public DialogComponentStringSelection getCompressionComponent() {
        return m_compressionComponent;
    }

    /**
     * @return the compressionModel
     */
    public SettingsModelString getCompressionModel() {
        return m_compressionModel;
    }

    /**
     * @return the chunkSizeComponent
     */
    public DialogComponentNumber getChunkSizeComponent() {
        return m_chunkSizeComponent;
    }

    /**
     * @return the chunkSizeModel
     */
    public SettingsModelInteger getChunkSizeModel() {
        return m_chunkSizeModel;
    }

    /**
     * @return the fileSizeComponent
     */
    public DialogComponentNumber getFileSizeComponent() {
        return m_fileSizeComponent;
    }

    /**
     * @return the fileSizeModel
     */
    public SettingsModelLong getFileSizeModel() {
        return m_fileSizeModel;
    }
}
