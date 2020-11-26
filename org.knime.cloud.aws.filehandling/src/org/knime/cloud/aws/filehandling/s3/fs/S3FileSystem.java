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

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import org.knime.cloud.aws.filehandling.s3.AwsUtils;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.SSEMode;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

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

    private final S3Client m_client;

    private final boolean m_normalizePaths;
    private final boolean m_hasListBucketPermission;

    private final boolean m_sseEnabled;
    private final SSEMode m_sseMode;
    private final String m_kmsKeyId;

    /**
     * Constructs an S3FileSystem for the given URI
     *
     * @param connectionInformation the {@link CloudConnectionInformation}
     * @param settings The node settings
     * @param cacheTTL The time to live for cache entries in the attributes cache
     */
    public S3FileSystem(final CloudConnectionInformation connectionInformation, final S3ConnectorNodeSettings settings,
        final long cacheTTL) {

        super(new S3FileSystemProvider(), //
            connectionInformation.toURI(), //
            cacheTTL, //
            settings.getWorkingDirectory(), //
            createFSLocationSpec(connectionInformation));

        m_normalizePaths = settings.getNormalizePath();
        m_sseEnabled = settings.isSseEnabled();
        m_sseMode = settings.getSseMode();
        m_kmsKeyId = settings.sseKmsUseAwsManaged() ? "" : settings.getKmsKeyId();
        try {
            m_client = createClient(settings, connectionInformation);
            m_hasListBucketPermission = testListBucketPermissions();
        } catch (final Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private boolean testListBucketPermissions() {
        try {
            m_client.listBuckets();
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 403) {
                return false;
            }

            throw e;
        }
    }

    /**
     * Creates an {@link FSLocationSpec} for an S3 file system with the given connection information.
     *
     * @param connectionInformation
     * @return an {@link FSLocationSpec} for an S3 file system with the given connection information.
     */
    public static FSLocationSpec createFSLocationSpec(final CloudConnectionInformation connectionInformation) {
        // the connection information is the AWS region
        final String specifier = String.format("%s:%s", FS_TYPE, connectionInformation.getHost());
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, specifier);
    }

    private static S3Client createClient(final S3ConnectorNodeSettings settings, final CloudConnectionInformation con) {
        Duration socketTimeout = Duration.ofSeconds(settings.getSocketTimeout());
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()//
            .connectionTimeout(Duration.ofMillis(con.getTimeout()))//
            .socketTimeout(socketTimeout)//
            .connectionTimeToLive(socketTimeout);

        return S3Client.builder()//
            .region(Region.of(con.getHost()))//
            .credentialsProvider(AwsUtils.getCredentialProvider(con))//
            .httpClientBuilder(httpClientBuilder)//
            .build();
    }

    /**
     * Sets up necessary SSE parameters in case server-side encryption is enabled.
     *
     * @param request {@link PutObjectRequest} builder.
     */
    public void populateSseParams(final PutObjectRequest.Builder request) {
        if (m_sseEnabled) {
            request.serverSideEncryption(m_sseMode.getEncryption());

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                request.ssekmsKeyId(m_kmsKeyId);
            }
        }
    }

    /**
     * Sets up necessary SSE parameters in case server-side encryption is enabled.
     *
     * @param request {@link CopyObjectRequest} builder.
     */
    public void populateSseParams(final CopyObjectRequest.Builder request) {
        if (m_sseEnabled) {
            request.serverSideEncryption(m_sseMode.getEncryption());

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                request.ssekmsKeyId(m_kmsKeyId);
            }
        }
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
     * @return the {@link S3Client} client for this file system
     */
    public S3Client getClient() {
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

    /**
     * @return <code>true</code> if the current account has enough permissions to perform <code>listBuckets</code> call,
     *         otherwise <code>false</code>.
     */
    public boolean hasListBucketPermission() {
        return m_hasListBucketPermission;
    }
}
