/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   20.08.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.cloud.aws.filehandling.s3.fs;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

import org.knime.cloud.aws.filehandling.s3.MultiRegionS3Client;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

/**
 * The Amazon S3 implementation of the {@link FileSystem} interface.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FileSystem extends BaseFileSystem<S3Path> {

    /**
     * Path separator for the S3 file system.
     */
    public static final String PATH_SEPARATOR = "/";

    /**
     * Amazon S3 URI scheme
     */
    public static final String FS_TYPE = "amazon-s3";

    private final MultiRegionS3Client m_client;

    private final boolean m_normalizePaths;

    /**
     * Constructs an S3FileSystem for the given URI
     *
     * @param connectionInformation the {@link CloudConnectionInformation}
     * @param settings The node settings
     * @param cacheTTL The time to live for cache entries in the attributes cache
     * @param credentials The credential provider.
     */
    public S3FileSystem(final CloudConnectionInformation connectionInformation, final S3ConnectorNodeSettings settings,
        final long cacheTTL, final CredentialsProvider credentials) {

        super(new S3FileSystemProvider(), //
            connectionInformation.toURI(), //
            cacheTTL, //
            settings.getWorkingDirectory(), //
            createFSLocationSpec());

        m_normalizePaths = settings.shouldNormalizePath();
        try {
            m_client = new MultiRegionS3Client(settings, connectionInformation, credentials);
        } catch (final RuntimeException | NoSuchAlgorithmException | IOException | InvalidSettingsException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Creates an {@link FSLocationSpec} for an S3 file system.
     *
     * @return an {@link FSLocationSpec} for an S3 file system.
     */
    public static FSLocationSpec createFSLocationSpec() {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, FS_TYPE);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

    /**
     * @return the {@link MultiRegionS3Client} instance.
     */
    public MultiRegionS3Client getClient() {
        return m_client;
    }

    @Override
    public S3Path getPath(final String first, final String... more) {
        return new S3Path(this, first, more);
    }

    @Override
    public void prepareClose() {
        m_client.close();
    }

    /**
     * @return whether to normalize paths.
     */
    public boolean normalizePaths() {
        return m_normalizePaths;
    }

}
