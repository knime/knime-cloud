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
 *   2020-12-13 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.SSEMode;
import org.knime.cloud.core.util.port.CloudConnectionInformation;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Class provides access to S3 API with several additional features:
 * <ul>
 * <li>
 * Multiregion support. Additional {@link S3Client} instances are created for accessing buckets from the different
 * regions (when needed). Appropriate client is selected to make calls depending on the bucket region.
 * </li>
 * <li>
 * Server-side encryption. Appropriate headers included when necessary if SSE is enabled in settings
 * </li>
 * <li>
 * Workaround for 'list-buckets' permission restrictions. <code>getBucket</code> method is implemented that way that
 * a dummy {@link Bucket} object is returned in case when bucket exists, but cannot be retrieved since user is lacking
 * 'list-buckets' permission
 * </li>
 * </ul>
 *
 * @author Alexander Bondaletov
 */
public class MultiRegionS3Client implements AutoCloseable {
    private final Duration m_socketTimeout;
    private final CloudConnectionInformation m_connectionInfo;

    private final boolean m_sseEnabled;
    private final SSEMode m_sseMode;
    private final String m_kmsKeyId;

    private final Map<String, Region> m_regionByBucket;
    private final Map<Region, S3Client> m_clientByRegion;

    private final S3Client m_defaultClient;
    private final S3Client m_pathStyleClient;

    private final boolean m_hasListBucketPermission;

    /**
     * @param settings The node settings.
     * @param connectionInfo The connection information
     *
     */
    public MultiRegionS3Client(final S3ConnectorNodeSettings settings,
        final CloudConnectionInformation connectionInfo) {
        m_socketTimeout = Duration.ofSeconds(settings.getSocketTimeout());
        m_connectionInfo = connectionInfo;

        m_sseEnabled = settings.isSseEnabled();
        m_sseMode = settings.getSseMode();
        m_kmsKeyId = settings.sseKmsUseAwsManaged() ? "" : settings.getKmsKeyId();

        m_regionByBucket = new HashMap<>();
        m_clientByRegion = new HashMap<>();

        Region region = Region.of(connectionInfo.getHost());
        m_defaultClient = getClientForRegion(region);
        m_pathStyleClient = createClientForRegion(region, true);

        m_hasListBucketPermission = testListBucketPermissions();
    }

    private S3Client getClientForRegion(final Region region) {
        return m_clientByRegion.computeIfAbsent(region, r -> createClientForRegion(r, false));
    }

    private S3Client createClientForRegion(final Region region, final boolean pathStyleAccess) {
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()//
            .connectionTimeout(Duration.ofMillis(m_connectionInfo.getTimeout()))//
            .socketTimeout(m_socketTimeout)//
            .connectionTimeToLive(m_socketTimeout);

        S3ClientBuilder builder = S3Client.builder()//
            .region(region)//
            .credentialsProvider(AwsUtils.getCredentialProvider(m_connectionInfo))//
            .httpClientBuilder(httpClientBuilder);//

        if (pathStyleAccess) {
            builder.serviceConfiguration(b -> b.pathStyleAccessEnabled(true));
        }

        return builder.build();
    }

    private boolean testListBucketPermissions() {
        try {
            listBuckets();
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 403) {
                return false;
            }

            throw e;
        }
    }

    /**
     * List buckets.
     *
     * @return The list buckets response.
     */
    public ListBucketsResponse listBuckets() {
        return m_defaultClient.listBuckets();
    }

    /**
     * @param request The request object.
     * @return The list objects response.
     */
    @SuppressWarnings("resource")
    public ListObjectsV2Response listObjects(final ListObjectsV2Request request) {
        return getClientForBucket(request.bucket()).listObjectsV2(request);
    }

    /**
     * Returns the {@link Bucket} object or <code>null</code> if bucket does not exist. Empty {@link Bucket} object is
     * returned in case user lacks 'list-bucket' permission.
     *
     * @param bucket The bucket name.
     * @return The {@link Bucket} object.
     */
    public Bucket getBucket(final String bucket) {
        if (m_hasListBucketPermission) {
            return fetchBucket(bucket);
        } else {
            return doesBucketExist(bucket) ? Bucket.builder().build() : null;
        }
    }

    private Bucket fetchBucket(final String bucket) {
        for (final Bucket buck : listBuckets().buckets()) {
            if (buck.name().equals(bucket)) {
                return buck;
            }
        }
        return null;
    }

    private boolean doesBucketExist(final String bucket) {
        return getRegionForBucket(bucket) != null;
    }

    /**
     * @param bucket The bucket name.
     * @param key The object key.
     * @return The head object response.
     */
    @SuppressWarnings("resource")
    public HeadObjectResponse headObject(final String bucket, final String key) {
        try {
            return getClientForBucket(bucket).headObject(b -> b.bucket(bucket).key(key));
        } catch (NoSuchKeyException ex) {//NOSONAR object doesn't exist
            return null;
        }
    }

    /**
     * @param bucket The bucket name.
     * @param key The object key.
     * @return <code>true</code> if object exists, <code>false</code> otherwise.
     */
    public boolean doesObjectExist(final String bucket, final String key) {
        return headObject(bucket, key) != null;
    }

    /**
     * @param bucket The bucket name.
     * @param key The object key.
     * @return The input stream for the object.
     */
    @SuppressWarnings("resource")
    public ResponseInputStream<GetObjectResponse> getObject(final String bucket, final String key) {
        return getClientForBucket(bucket).getObject(b -> b.bucket(bucket).key(key));
    }

    /**
     * Creates a bucket.
     *
     * @param bucket The bucket name.
     */
    public void createBucket(final String bucket) {
        m_defaultClient.createBucket(b -> b.bucket(bucket));
    }

    /**
     * Puts and object.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     * @param body The request body.
     */
    @SuppressWarnings("resource")
    public void putObject(final String bucket, final String key, final RequestBody body) {
        PutObjectRequest req = PutObjectRequest.builder()//
            .bucket(bucket)//
            .key(key)//
            .applyMutation(this::populateSseParams)//
            .build();

        getClientForBucket(bucket).putObject(req, body);
    }

    /**
     * Performs copy of the object.
     *
     * @param srcBucket The bucket name of the source object.
     * @param srcKey The object key of the source object.
     * @param dstBucket The bucket name of the destination object.
     * @param dstKey The object key of the destination object.
     */
    @SuppressWarnings("resource")
    public void copyObject(final String srcBucket, final String srcKey, final String dstBucket, final String dstKey) {
        CopyObjectRequest req = CopyObjectRequest.builder()//
            .copySource(encodeCopySource(srcBucket, srcKey)).destinationBucket(dstBucket)//
            .destinationKey(dstKey)//
            .applyMutation(this::populateSseParams)//
            .build();

        getClientForBucket(dstBucket).copyObject(req);
    }

    private static String encodeCopySource(final String bucket, final String blob) {
        try {
            return URLEncoder.encode(bucket + "/" + blob, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException ex) {//NOSONAR should not happen
            return null;
        }
    }

    /**
     * Deletes the object.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     */
    @SuppressWarnings("resource")
    public void deleteObject(final String bucket, final String key) {
        getClientForBucket(bucket).deleteObject(b -> b.bucket(bucket).key(key));
    }

    /**
     * Deletes the bucket.
     *
     * @param bucket The bucket name.
     */
    @SuppressWarnings("resource")
    public void deleteBucket(final String bucket) {
        getClientForBucket(bucket).deleteBucket(b -> b.bucket(bucket));
    }

    private S3Client getClientForBucket(final String bucket) {
        if (bucket == null) {
            return m_defaultClient;
        }

        Region region = getRegionForBucket(bucket);

        if (region == null) {
            return m_defaultClient;
        }

        return getClientForRegion(region);
    }

    private Region getRegionForBucket(final String bucket) {
        return m_regionByBucket.computeIfAbsent(bucket, this::fetchRegionForBucket);
    }

    private Region fetchRegionForBucket(final String bucket) {
        Region region = null;

        try {
            String location = m_pathStyleClient.getBucketLocation(b -> b.bucket(bucket)).locationConstraintAsString();

            //Javadoc for getBucketLocation states that
            //  'Buckets in Region us-east-1 have a LocationConstraint of null.'
            //But, in fact, it is an empty string, so we check for both cases.
            if (location == null || location.isEmpty()) {
                region = Region.US_EAST_1;
            } else {
                region = Region.of(location);
            }
        } catch (NoSuchBucketException ex) {//NOSONAR
            // Bucket does not exist
        }

        return region;
    }

    private void populateSseParams(final PutObjectRequest.Builder request) {
        if (m_sseEnabled) {
            request.serverSideEncryption(m_sseMode.getEncryption());

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                request.ssekmsKeyId(m_kmsKeyId);
            }
        }
    }

    private void populateSseParams(final CopyObjectRequest.Builder request) {
        if (m_sseEnabled) {
            request.serverSideEncryption(m_sseMode.getEncryption());

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                request.ssekmsKeyId(m_kmsKeyId);
            }
        }
    }

    @Override
    public void close() {
        for (S3Client client : m_clientByRegion.values()) {
            client.close();
        }
        m_pathStyleClient.close();
    }
}
