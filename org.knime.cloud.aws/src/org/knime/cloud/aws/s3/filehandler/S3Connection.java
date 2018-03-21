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
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * S3 Connection
 *
 * @author Budi Yanto, KNIME.com, Berlin, Germany
 *
 */
public class S3Connection extends Connection {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(S3Connection.class);

	private final CloudConnectionInformation m_connectionInformation;

	private AmazonS3Client m_client;

	private TransferManager m_transferManager;

	private List<String> m_bucketsCache;


	public S3Connection(final CloudConnectionInformation connectionInformation) {
		m_connectionInformation = connectionInformation;
		m_bucketsCache = new ArrayList<String>();
	}

	@Override
	public void open() throws Exception {
		if (!isOpen()) {
			final String accessKeyId = m_connectionInformation.getUser();
			final String secretAccessKey = KnimeEncryption.decrypt(m_connectionInformation.getPassword());

			LOGGER.info("Create a new AmazonS3Client in Region \"" + m_connectionInformation.getHost()
					+ "\" with connection timeout " + m_connectionInformation.getTimeout() + " milliseconds");

			try {
			    final ClientConfiguration clientConfig = new ClientConfiguration().withConnectionTimeout(
			        m_connectionInformation.getTimeout());
			    if(m_connectionInformation.useKeyChain()) {
			        m_client = new AmazonS3Client(clientConfig).withRegion(
			            Regions.fromName(m_connectionInformation.getHost()));
			    } else {
			        final AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
			        m_client = new AmazonS3Client(credentials, clientConfig).withRegion(
			            Regions.fromName(m_connectionInformation.getHost()));
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
				    } else {
				        throw e;
				    }
				}
				m_transferManager = new TransferManager(m_client);
			} catch (final AmazonS3Exception ex) {
				close();
				throw ex;
			}
		}
	}

	@Override
	public boolean isOpen() {
		if (m_client != null && m_transferManager != null) {
			return true;
		}
		return false;
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
	public AmazonS3Client getClient() {
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
		if (getBuckets().contains(bucketName)) {
			return true;
		}
		return false;
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

}
