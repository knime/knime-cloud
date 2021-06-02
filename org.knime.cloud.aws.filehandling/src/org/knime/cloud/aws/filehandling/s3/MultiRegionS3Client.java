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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings;
import org.knime.cloud.aws.filehandling.s3.node.S3ConnectorNodeSettings.SSEMode;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.CredentialsProvider;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/**
 * Class provides access to S3 API with several additional features:
 * <ul>
 * <li>Multiregion support. Additional {@link S3Client} instances are created for accessing buckets from the different
 * regions (when needed). Appropriate client is selected to make calls depending on the bucket region.</li>
 * <li>Server-side encryption. Appropriate headers included when necessary if SSE is enabled in settings</li>
 * <li>Workaround for 'list-buckets' permission restrictions. <code>getBucket</code> method is implemented that way that
 * a dummy {@link Bucket} object is returned in case when bucket exists, but cannot be retrieved since user is lacking
 * 'list-buckets' permission</li>
 * </ul>
 *
 * @author Alexander Bondaletov
 */
public class MultiRegionS3Client implements AutoCloseable {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(MultiRegionS3Client.class);

    private static final String AES256 = "AES256";

    private final Duration m_socketTimeout;

    private final CloudConnectionInformation m_connectionInfo;

    private final boolean m_sseEnabled;

    private final SSEMode m_sseMode;

    private final String m_kmsKeyId;

    private final String m_customerKey;
    private final String m_customerKeyMD5;

    private final Map<String, Region> m_regionByBucket;

    private final Map<Region, S3Client> m_clientByRegion;

    private final S3Client m_defaultClient;

    private final S3Client m_pathStyleClient;

    private final boolean m_hasListBucketPermission;

    /**
     * @param settings The node settings.
     * @param connectionInfo The connection information
     * @param credentials Credentials provider.
     * @throws InvalidSettingsException When {@link InvalidSettingsException} happens during loading customer-provided
     *             encryption key.
     * @throws IOException When {@link IOException} happens during loading customer-provided encryption key.
     *
     */
    public MultiRegionS3Client(final S3ConnectorNodeSettings settings, final CloudConnectionInformation connectionInfo,
        final CredentialsProvider credentials) throws IOException, InvalidSettingsException {
        m_socketTimeout = Duration.ofSeconds(settings.getSocketTimeout());
        m_connectionInfo = connectionInfo;

        m_sseEnabled = settings.isSseEnabled();
        m_sseMode = settings.getSseMode();
        m_kmsKeyId = settings.sseKmsUseAwsManaged() ? "" : settings.getKmsKeyId();

        m_customerKey = settings.getCustomerKey(credentials);
        m_customerKeyMD5 = computeCustomerKeyMD5(m_customerKey);

        m_regionByBucket = new ConcurrentHashMap<>();
        m_clientByRegion = new ConcurrentHashMap<>();

        Region region = Region.of(connectionInfo.getHost());
        m_defaultClient = getClientForRegion(region);
        m_pathStyleClient = createClientForRegion(region, true);

        m_hasListBucketPermission = testListBucketPermissions();
    }

    private static String computeCustomerKeyMD5(final String customerKey)
        throws InvalidSettingsException {
        if (customerKey == null) {
            return null;
        }

        byte[] keyBytes = AwsUtils.getCustomerKeyBytes(customerKey);
        byte[] md5;
        try {
            md5 = MessageDigest.getInstance("MD5").digest(keyBytes);
        } catch (NoSuchAlgorithmException ex) {
            // should never happen, therefore we make it a RuntimeException
            throw new IllegalStateException(ex.getMessage(), ex);
        }

        return Base64.getEncoder().encodeToString(md5);
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
        boolean canListBucketsInAccount = false;

        if (!m_connectionInfo.isUseAnonymous()) {
            try {
                listBuckets();
                canListBucketsInAccount = true;
            } catch (S3Exception e) {
                if (e.statusCode() != 403) {
                    throw e;
                }
            }
        }

        return canListBucketsInAccount;
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
        HeadObjectRequest.Builder builder = HeadObjectRequest.builder().bucket(bucket).key(key);
        S3Client client = getClientForBucket(bucket);

        if (m_sseEnabled && m_sseMode == SSEMode.CUSTOMER_PROVIDED) {
            HeadObjectRequest req = builder.copy()//
                .sseCustomerAlgorithm(AES256)//
                .sseCustomerKey(m_customerKey)//
                .sseCustomerKeyMD5(m_customerKeyMD5)//
                .build();

            try {
                return headObject(req, client);
            } catch (S3Exception ex) {
                if (ex.statusCode() == 400) {
                    //do nothing, possibly object is not encrypted, will retry request without SSE params
                    LOGGER.warnWithFormat(
                        "Failed to query metadata for /%s/%s using SSE-C params. Retrying request without encryption enabled.",
                        bucket, key);
                } else {
                    throw ex;
                }
            }
        }

        return headObject(builder.build(), client);
    }

    private static HeadObjectResponse headObject(final HeadObjectRequest request, final S3Client client) {
        try {
            return client.headObject(request);
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
     * Get object using SSE-C parameters if present in settings and fallback on request without encryption parameters on
     * error code 400 to read a possible not encrypted object.
     *
     * @param bucket The bucket name.
     * @param key The object key.
     * @return The input stream for the object.
     */
    @SuppressWarnings("resource")
    public ResponseInputStream<GetObjectResponse> getObject(final String bucket, final String key) {
        GetObjectRequest.Builder builder = GetObjectRequest.builder()//
            .bucket(bucket)//
            .key(key);
        S3Client client = getClientForBucket(bucket);


        if (m_sseEnabled && m_sseMode == SSEMode.CUSTOMER_PROVIDED) {
            GetObjectRequest sseReq = builder.copy() //
                .sseCustomerAlgorithm(AES256) //
                .sseCustomerKey(m_customerKey) //
                .sseCustomerKeyMD5(m_customerKeyMD5) //
                .build();

            try {
                return client.getObject(sseReq);
            } catch (S3Exception ex) {
                if (ex.statusCode() == 400) {
                    // possibly object is not encrypted, retry request without SSE params
                    LOGGER.warnWithFormat(
                        "Failed to get object /%s/%s using SSE-C params. Retrying request without encryption enabled.",
                        bucket, key);
                } else {
                    throw ex;
                }
            }
        }

        return client.getObject(builder.build()); // rerun request without SSE-C params
    }

    private boolean checkCustomerEncryption(final String bucket, final String blob) throws IOException {
        HeadObjectResponse metadata = headObject(bucket, blob);
        return metadata.sseCustomerAlgorithm() != null;
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
        PutObjectRequest.Builder builder = PutObjectRequest.builder()//
            .bucket(bucket)//
            .key(key);

        if (m_sseEnabled) {
            if (m_sseMode == SSEMode.S3 || m_sseMode == SSEMode.KMS) {
                builder.serverSideEncryption(m_sseMode.getEncryption());
            }

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                builder.ssekmsKeyId(m_kmsKeyId);
            }

            if (m_sseMode == SSEMode.CUSTOMER_PROVIDED) {
                builder.sseCustomerAlgorithm(AES256);
                builder.sseCustomerKey(m_customerKey);
                builder.sseCustomerKeyMD5(m_customerKeyMD5);
            }
        }

        getClientForBucket(bucket).putObject(builder.build(), body);
    }

    /**
     * Performs copy of the object.
     *
     * @param srcBucket The bucket name of the source object.
     * @param srcKey The object key of the source object.
     * @param dstBucket The bucket name of the destination object.
     * @param dstKey The object key of the destination object.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public void copyObject(final String srcBucket, final String srcKey, final String dstBucket, final String dstKey)
        throws IOException {
        CopyObjectRequest.Builder builder = CopyObjectRequest.builder()//
            .copySource(encodeCopySource(srcBucket, srcKey))//
            .destinationBucket(dstBucket)//
            .destinationKey(dstKey);

        if (m_sseEnabled) {
            if (m_sseMode == SSEMode.S3 || m_sseMode == SSEMode.KMS) {
                builder.serverSideEncryption(m_sseMode.getEncryption());
            }

            if (m_sseMode == SSEMode.KMS && !m_kmsKeyId.isEmpty()) {
                builder.ssekmsKeyId(m_kmsKeyId);
            }

            if (m_sseMode == SSEMode.CUSTOMER_PROVIDED) {
                if (checkCustomerEncryption(srcBucket, srcKey)) {
                    builder.copySourceSSECustomerAlgorithm(AES256);
                    builder.copySourceSSECustomerKey(m_customerKey);
                    builder.copySourceSSECustomerKeyMD5(m_customerKeyMD5);
                }

                builder.sseCustomerAlgorithm(AES256);
                builder.sseCustomerKey(m_customerKey);
                builder.sseCustomerKeyMD5(m_customerKeyMD5);
            }
        }

        getClientForBucket(dstBucket).copyObject(builder.build());
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

    /**
     * First, try to fetch the region by getBucketLocation. This returns HttpStatusCode.FORBIDDEN if
     * s3:GetBucketLocation permission is not there. In this case, try again with headBucket which requires no
     * permissions (according to the documentation if requires s3:ListBucket, but S3 behaves differently).
     *
     * @param bucket
     * @return null when the bucket does not exist
     * @throws SdkExceptionwhen if the bucket exists but the region could not be retrieved.
     */
    private Region fetchRegionForBucket(final String bucket) {
        try {
            return fetchRegionWithGetBucketLocation(bucket);
        } catch (S3Exception e) { // NOSONAR can be ignored, because we use headBucket as fallback
            return fetchRegionWithHeadBucket(bucket);
        }
    }

    private Region fetchRegionWithGetBucketLocation(final String bucket) {
        Region region = null;

        try {
            // requires s3:GetBucketLocation permission
            final String location = m_pathStyleClient.getBucketLocation(b -> b.bucket(bucket)).locationConstraintAsString();

            // Javadoc for getBucketLocation states that
            // 'Buckets in Region us-east-1 have a LocationConstraint of null.'
            // But, in fact, it is an empty string, so we check for both cases.
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

    private Region fetchRegionWithHeadBucket(final String bucket) {
        S3Exception exception = null;

        Map<String, List<String>> responseHeaders;
        try {
            // does not require any permission at all
            responseHeaders = m_defaultClient.headBucket(b -> b.bucket(bucket)).sdkHttpResponse().headers();
        } catch (NoSuchBucketException ex) { //NOSONAR the bucket doesn't exist
            return null;
        } catch (S3Exception ex) { // NOSONAR
            // headBucket often returns HTTP 301/307, but the headers contain the region
            responseHeaders = ex.awsErrorDetails().sdkHttpResponse().headers();
            exception = ex;
        }

        if (responseHeaders.containsKey("x-amz-bucket-region")) {
            return Region.of(responseHeaders.get("x-amz-bucket-region").get(0));
        } else if (exception != null){
            throw exception;
        } else {
            throw SdkException.builder().message("Could not determine region of bucket " + bucket).build();
        }
    }

    @Override
    public void close() {
        for (S3Client client : m_clientByRegion.values()) {
            client.close();
        }
        m_pathStyleClient.close();
    }

    /**
     * Return an object for S3Presigner for request S3 bucket. The bucket name is required because the S3Presigner
     * depends on S3 Region information
     *
     * @param bucketName Name of the S3 against which the presigner should be issued
     *
     * @return An object of S3Presigner
     */
    public S3Presigner getS3Presigner(final String bucketName) {
        final Region region = getRegionForBucket(bucketName);
        if (region == null) {
            throw NoSuchBucketException.builder().message("The specified bucket does not exist").build();
        }
        AwsCredentialsProvider awsCreds = AwsUtils.getCredentialProvider(m_connectionInfo);
        return S3Presigner.builder().credentialsProvider(awsCreds).region(region).build();
    }
}
