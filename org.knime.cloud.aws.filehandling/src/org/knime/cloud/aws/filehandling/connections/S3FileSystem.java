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
package org.knime.cloud.aws.filehandling.connections;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.util.KnimeEncryption;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

/**
 * The Amazon S3 implementation of the {@link FileSystem} interface.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class S3FileSystem extends BaseFileSystem {

    private final AmazonS3 m_client;

    private final boolean m_normalizePaths;

    private static final String PATH_SEPARATOR = "/";

    /**
     * Constructs an S3FileSystem for the given URI
     *
     * @param provider the {@link S3FileSystemProvider}
     * @param uri the URI for the file system
     * @param env the environment map
     * @param connectionInformation the {@link CloudConnectionInformation}
     * @param timeToLive the time to live for cache entries in the attributes cache
     * @param normalizePaths whether paths should be normalized
     */
    public S3FileSystem(final S3FileSystemProvider provider, final URI uri, final Map<String, ?> env,
        final CloudConnectionInformation connectionInformation, final long timeToLive, final boolean normalizePaths) {
        super(provider, uri, "S3 file system", "S3 file system", timeToLive);
        m_normalizePaths = normalizePaths;
        try {
            if (connectionInformation.switchRole()) {
                m_client = getRoleAssumedS3Client(connectionInformation, provider.getClientConfig());
            } else {
                m_client = getS3Client(connectionInformation, provider.getClientConfig());
            }
        } catch (final Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private static AmazonS3 getS3Client(final CloudConnectionInformation connectionInformation,
        final ClientConfiguration clientConfig) throws Exception {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withClientConfiguration(clientConfig)
            .withRegion(connectionInformation.getHost());

        if (!connectionInformation.useKeyChain()) {
            final AWSCredentials credentials = getCredentials(connectionInformation);
            builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
        }

        return builder.build();
    }

    private static AmazonS3 getRoleAssumedS3Client(final CloudConnectionInformation connectionInformation,
        final ClientConfiguration clientConfig) throws Exception {
        final AWSSecurityTokenServiceClientBuilder builder =
            AWSSecurityTokenServiceClientBuilder.standard().withRegion(connectionInformation.getHost());
        if (!connectionInformation.useKeyChain()) {
            final AWSCredentials credentials = getCredentials(connectionInformation);
            builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
        }

        final AWSSecurityTokenService stsClient = builder.build();

        final AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleArn(buildARN(connectionInformation))
            .withDurationSeconds(3600).withRoleSessionName("KNIME_S3_Connection");

        final AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRoleRequest);

        final BasicSessionCredentials tempCredentials =
            new BasicSessionCredentials(assumeResult.getCredentials().getAccessKeyId(),
                assumeResult.getCredentials().getSecretAccessKey(), assumeResult.getCredentials().getSessionToken());

        return AmazonS3ClientBuilder.standard().withClientConfiguration(clientConfig)
            .withCredentials(new AWSStaticCredentialsProvider(tempCredentials))
            .withRegion(connectionInformation.getHost()).build();
    }

    private static AWSCredentials getCredentials(final CloudConnectionInformation connectionInformation)
        throws Exception {

        if (connectionInformation.isUseAnonymous()) {
            return new AnonymousAWSCredentials();
        }
        final String accessKeyId = connectionInformation.getUser();
        final String secretAccessKey = KnimeEncryption.decrypt(connectionInformation.getPassword());

        return new BasicAWSCredentials(accessKeyId, secretAccessKey);

    }

    private static String buildARN(final CloudConnectionInformation connectionInformation) {
        return "arn:aws:iam::" + connectionInformation.getSwitchRoleAccount() + ":role/"
            + connectionInformation.getSwitchRoleName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(new S3Path(this, PATH_SEPARATOR));
    }

    /**
     * @return the {@link AmazonS3} client for this file system
     */
    public AmazonS3 getClient() {
        return m_client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path getPath(final String first, final String... more) {
        if (more.length == 0) {
            return new S3Path(this, first);
        } else {
            final StringBuilder sb = new StringBuilder(first);
            for (final String subPath : more) {
                if (!(PATH_SEPARATOR.charAt(0) == sb.charAt(sb.length() - 1) || subPath.startsWith(PATH_SEPARATOR))) {
                    sb.append(PATH_SEPARATOR);
                }
                sb.append(subPath);
            }
            return new S3Path(this, sb.toString());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareClose() {
        m_client.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSchemeString() {
        return "s3";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHostString() {
        return null;
    }

    /**
     * @return whether to normalize paths.
     */
    public boolean normalizePaths() {
        return m_normalizePaths;
    }

    @Override
    protected String getCachedAttributesKey(final Path path) {
        return path.toString();
    }
}
