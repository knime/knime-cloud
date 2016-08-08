package org.knime.cloud.aws.s3.filehandler;

import java.util.ArrayList;
import java.util.List;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
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

	private final ConnectionInformation m_connectionInformation;

	private AmazonS3Client m_client;

	private TransferManager m_transferManager;

	private List<String> m_bucketsCache;

	public S3Connection(final ConnectionInformation connectionInformation) {
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
			final AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
			m_client = new AmazonS3Client(credentials,
					new ClientConfiguration().withConnectionTimeout(m_connectionInformation.getTimeout()))
							.withRegion(Regions.fromName(m_connectionInformation.getHost()));
			try {
				resetCache();
				getBuckets();
				m_transferManager = new TransferManager(m_client);
			} catch (final Exception ex) {
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
		m_bucketsCache.clear();
		m_transferManager.shutdownNow();
		m_client.shutdown();
	}

	public AmazonS3Client getClient() {
		return m_client;
	}

	public TransferManager getTransferManager() {
		return m_transferManager;
	}

	public List<String> getBuckets() throws Exception {
		if (m_bucketsCache == null) {
			m_bucketsCache = new ArrayList<String>();
			for (final Bucket bucket : m_client.listBuckets()) {
				m_bucketsCache.add(bucket.getName());
			}
		}
		return m_bucketsCache;
	}

	public void validateBucketName(final String bucketName) throws Exception {
		open();
		if (!getBuckets().contains(bucketName)) {
			throw new IllegalArgumentException("Not authorized to access the bucket: " + bucketName);
		}
	}

	public boolean isOwnBucket(final String bucketName) throws Exception {
		open();
		if (getBuckets().contains(bucketName)) {
			return true;
		}
		return false;
	}

	public void resetCache() {
		if (m_bucketsCache != null) {
			m_bucketsCache.clear();
		}
		m_bucketsCache = null;
	}

	// public void resetBuckets() throws Exception {
	// open();
	// m_bucketsCache.clear();
	// for (final Bucket bucket : getClient().listBuckets()) {
	// m_bucketsCache.add(bucket.getName());
	// }
	// }

	// public boolean doesBucketExist(final String bucketName) throws Exception
	// {
	// open();
	// if (m_client.doesBucketExist(bucketName) &&
	// m_buckets.contains(bucketName)) {
	// return true;
	// }
	// return false;
	// }

}
