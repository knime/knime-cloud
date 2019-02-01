package org.knime.cloud.aws.s3.filehandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

/**
 * S3 Connection
 *
 * @author Budi Yanto, KNIME.com, Berlin, Germany
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 *
 */
public class S3Connection extends Connection {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(S3Connection.class);

	private final CloudConnectionInformation m_connectionInformation;

	private AmazonS3 m_client;

	private TransferManager m_transferManager;

	private List<String> m_bucketsCache;


	private boolean m_restrictedPermissions = false;

	/**
	 * Creates an S3 connection, given the connection information.
	 *
	 * @param connectionInformation The connection information for this S3 connection
	 */
	public S3Connection(final CloudConnectionInformation connectionInformation) {
		m_connectionInformation = connectionInformation;
		m_bucketsCache = new ArrayList<>();
	}

	@Override
	public void open() throws Exception {
		if (!isOpen()) {
			LOGGER.info("Create a new AmazonS3Client in Region \"" + m_connectionInformation.getHost()
					+ "\" with connection timeout " + m_connectionInformation.getTimeout() + " milliseconds");

			try {
		        if (m_connectionInformation.switchRole()) {
		            m_client = getRoleAssumedS3Client(m_connectionInformation);
		        } else {
                    m_client = getS3Client(m_connectionInformation);
		        }

				resetCache();
				try {
				    // Try to fetch buckets. Will not work if ListAllMyBuckets is set to false
				    getBuckets();
				} catch (final AmazonS3Exception e){
				    if (Objects.equals(e.getErrorCode(), "InvalidAccessKeyId")) {
				        throw new InvalidSettingsException("Check your Access Key ID / Secret Key.");
				    } else if (Objects.equals(e.getErrorCode(), "AccessDenied")) {
				        // do nothing, see AP-8279
				        // This means that we do not have access to root level,
				        m_restrictedPermissions = true;
				    } else {
				        throw e;
				    }
				}
				m_transferManager = TransferManagerBuilder.standard().withS3Client(m_client).build();
			} catch (final AmazonS3Exception ex) {
				close();
				throw ex;
			}
		}
	}

	private static AmazonS3 getS3Client(final CloudConnectionInformation connectionInformation) throws Exception {
	    final ClientConfiguration clientConfig = new ClientConfiguration().withConnectionTimeout(
            connectionInformation.getTimeout());

	    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
	            .withClientConfiguration(clientConfig)
	            .withRegion(connectionInformation.getHost());

	    if (!connectionInformation.useKeyChain()) {
	        AWSCredentials credentials = getCredentials(connectionInformation);
	        builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
	    }


	    return builder.build();
	}

	private static AmazonS3 getRoleAssumedS3Client(final CloudConnectionInformation connectionInformation) throws Exception {
	    AWSSecurityTokenServiceClientBuilder builder = AWSSecurityTokenServiceClientBuilder.standard().
	            withRegion(connectionInformation.getHost());
	    if (!connectionInformation.useKeyChain()) {
	        AWSCredentials credentials = getCredentials(connectionInformation);
            builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
	    }

	    AWSSecurityTokenService stsClient = builder.build();

        final AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
                .withRoleArn(buildARN(connectionInformation))
                .withDurationSeconds(3600)
                .withRoleSessionName("KNIME_S3_Connection");

        final AssumeRoleResult assumeResult = stsClient.assumeRole(assumeRoleRequest);

        final BasicSessionCredentials tempCredentials =
                new BasicSessionCredentials(
                        assumeResult.getCredentials().getAccessKeyId(),
                        assumeResult.getCredentials().getSecretAccessKey(),
                        assumeResult.getCredentials().getSessionToken());

         return AmazonS3ClientBuilder.standard().withCredentials(
            new AWSStaticCredentialsProvider(tempCredentials))
                .withRegion(connectionInformation.getHost()).build();
	}

	private static String buildARN(final CloudConnectionInformation connectionInformation) {
	    return "arn:aws:iam::" + connectionInformation.getSwitchRoleAccount() + ":role/" + connectionInformation.getSwitchRoleName();
	}

	private static AWSCredentials getCredentials(final CloudConnectionInformation connectionInformation) throws Exception {
	    final String accessKeyId = connectionInformation.getUser();
        final String secretAccessKey = KnimeEncryption.decrypt(connectionInformation.getPassword());
        return new BasicAWSCredentials(accessKeyId, secretAccessKey);

	}

	@Override
	public boolean isOpen() {
		return m_client != null && m_transferManager != null;
	}

	@Override
	public void close() throws Exception {
		resetCache();
		if(m_transferManager != null) {
		    m_transferManager.shutdownNow();
		}
		if(m_client != null) {
		    m_client.shutdown();
		}
	}

	/**
	 * Get this connection's AmazonS3Client
	 *
	 * @return the connection's client
	 */
	public AmazonS3 getClient() {
		return m_client;
	}

	/**
	 * Get this connection's TransferManager
	 * @return the connection's transfer manager
	 */
	public TransferManager getTransferManager() {
		return m_transferManager;
	}

	/**
	 * Get the List of this connection's cached buckets
	 * @return the list of this connection's cached buckets
	 * @throws AmazonS3Exception
	 */
	public List<String> getBuckets() throws AmazonS3Exception {
		if (m_bucketsCache == null) {
			m_bucketsCache = new ArrayList<String>();
			for (final Bucket bucket : m_client.listBuckets()) {
				m_bucketsCache.add(bucket.getName());
			}
		}
		return m_bucketsCache;
	}

	/**
	 * Validate the given buckets name
	 * @param bucketName the bucket name to be validated
	 * @throws Exception
	 */
	public void validateBucketName(final String bucketName) throws Exception {
		open();
		if (!getBuckets().contains(bucketName)) {
			throw new IllegalArgumentException("Not authorized to access the bucket: " + bucketName);
		}
	}

	/**
	 * Check whether the bucket is accessible from the client
	 * @param bucketName the bucket that should be checked
	 * @return <code>true</code> if the bucket is accessible, <code>false</code> otherwise
	 * @throws Exception
	 */
	public boolean isOwnBucket(final String bucketName) throws Exception {
		open();
		return getBuckets().contains(bucketName);
	}

	/**
	 * Reset this connection's bucket cache
	 */
	public void resetCache() {
		if (m_bucketsCache != null) {
			m_bucketsCache.clear();
		}
		m_bucketsCache = null;
	}

	/**
	 * Returns whether to use server side encryption.
	 *
	 * @return Whether to use server side encryption
	 */
	public boolean useSSEncryption() {
        return m_connectionInformation.useSSEncryption();

	}

	/**
	 * Returns whether or not the connection was created with credentials that have restricted access to S3.
	 * This could be bucket-specific access without list-buckets permissions.
	 *
	 * @return Whether or not the connection was created with credentials that have restricted access to S3.
	 */
	public boolean restrictedPermissions() {
	    return m_restrictedPermissions;
	}

}
