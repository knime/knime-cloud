/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 *   Jul 19, 2016 (budiyanto): created
 */
package org.knime.cloud.aws.s3.filehandler;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.node.util.CheckUtils;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Implementation of {@link CloudRemoteFile} for Amazon S3
 *
 * @author Budi Yanto, KNIME.com
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class S3RemoteFile extends CloudRemoteFile<S3Connection> {

	private File m_tmpFileCache = null;


	protected S3RemoteFile(final URI uri, final ConnectionInformation connectionInformation,
			final ConnectionMonitor<S3Connection> connectionMonitor) {
		this(uri, connectionInformation, connectionMonitor, null);
	}

	protected S3RemoteFile(final URI uri, final ConnectionInformation connectionInformation,
			final ConnectionMonitor<S3Connection> connectionMonitor, final S3ObjectSummary summary) {
		super(uri, connectionInformation, connectionMonitor);
		CheckUtils.checkArgumentNotNull(connectionInformation, "Connection information must not be null");
		if (summary != null) {
			m_containerName = summary.getBucketName();
			m_blobName = summary.getKey();
			m_lastModified = summary.getLastModified().getTime();
			m_size = summary.getSize();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected S3Connection createConnection() {
		return new S3Connection(getConnectionInformation());
	}

	private AmazonS3Client getClient() throws Exception {
		return getOpenedConnection().getClient();
	}

	private TransferManager getTransferManager() throws Exception {
		return getOpenedConnection().getTransferManager();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doesContainerExist(String containerName) throws Exception {
		return getOpenedConnection().isOwnBucket(containerName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doestBlobExist(String containerName, String blobName) throws Exception {
		return getClient().doesObjectExist(containerName, blobName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected S3RemoteFile[] listRootFiles() throws Exception {
		final List<Bucket> buckets = getClient().listBuckets();
		if (buckets == null || buckets.isEmpty()) {
			return new S3RemoteFile[0];
		}
		final S3RemoteFile[] files = new S3RemoteFile[buckets.size()];
		for (int i = 0; i < files.length; i++) {
			final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
					getURI().getPort(), createContainerPath(buckets.get(i).getName()), getURI().getQuery(),
					getURI().getFragment());
			files[i] = new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor());
		}
		return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected S3RemoteFile[] listDirectoryFiles() throws Exception {
		final String bucketName = getContainerName();
		final String prefix = getBlobName();
		final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
				.withPrefix(prefix).withDelimiter(DELIMITER);
		ListObjectsV2Result result;
		final List<S3RemoteFile> fileList = new ArrayList<S3RemoteFile>();
		do {
			result = getClient().listObjectsV2(request);
			for (final S3ObjectSummary summary : result.getObjectSummaries()) {
				if (!summary.getKey().equals(prefix)) {
					final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
							getURI().getPort(), createContainerPath(bucketName) + summary.getKey(),
							getURI().getQuery(), getURI().getFragment());
					fileList.add(
							new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor(), summary));
				}
			}

			for (final String commPrefix : result.getCommonPrefixes()) {
				final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
						getURI().getPort(), createContainerPath(bucketName) + commPrefix, getURI().getQuery(),
						getURI().getFragment());
				fileList.add(new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor()));
			}

			request.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated());

		final S3RemoteFile[] files = fileList.toArray(new S3RemoteFile[fileList.size()]);
		// Arrays.sort(files);

	return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getBlobSize() throws Exception {
		return getClient().getObjectMetadata(getContainerName(), getBlobName()).getContentLength();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getLastModified() throws Exception {
		return getClient().getObjectMetadata(getContainerName(), getBlobName()).getLastModified().getTime();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteContainer() throws Exception {
		for (final CloudRemoteFile<S3Connection> file : listFiles()) {
			((S3RemoteFile) file).delete();
		}
		getClient().deleteBucket(getContainerName());
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteDirectory() throws Exception {
		for (final CloudRemoteFile<S3Connection> file : listFiles()) {
			((S3RemoteFile) file).delete();
		}
		return deleteBlob();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteBlob() throws Exception {
		getClient().deleteObject(getContainerName(), getBlobName());
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createContainer() throws Exception {
		getClient().createBucket(getContainerName());
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createDirectory(String dirName) throws Exception {
		final ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		// Create empty content
		final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		// Create a PutObjectRequest passing the folder name
		// suffixed by the DELIMITER
		final PutObjectRequest putObjectRequest = new PutObjectRequest(getContainerName(), dirName, emptyContent,
				metadata);
		// Send request to S3 to create folder
		getClient().putObject(putObjectRequest);
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputStream openInputStream() throws Exception {
		final String bucketName = getContainerName();
		final String key = getBlobName();
		final Download download = getTransferManager().download(bucketName, key, getTempFile());
		download.waitForCompletion();
		return new BufferedInputStream(new FileInputStream(getTempFile()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputStream openOutputStream() throws Exception {
		return new S3OutputStream(this);
	}

	private File getTempFile() throws Exception {
		if (m_tmpFileCache == null) {
			m_tmpFileCache = File.createTempFile("s3tmpfile", null);
			m_tmpFileCache.deleteOnExit();
		}
		return m_tmpFileCache;
	}

	@Override
	protected void resetCache() throws Exception {
		super.resetCache();
		getOpenedConnection().resetCache();
	}

	private class S3OutputStream extends OutputStream {

		private final S3RemoteFile m_file;
		private final OutputStream m_stream;

		private S3OutputStream(final S3RemoteFile file) throws Exception {
			m_file = file;
			m_stream = new BufferedOutputStream(new FileOutputStream(file.getTempFile()));
		}

		@Override
		public void write(final int b) throws IOException {
			m_stream.write(b);
		}

		@Override
		public void write(final byte[] b) throws IOException {
			m_stream.write(b);
		}

		@Override
		public void write(final byte[] b, final int off, final int len) throws IOException {
			m_stream.write(b, off, len);
		}

		@Override
		public void flush() throws IOException {
			m_stream.flush();
		}

		@Override
		public void close() throws IOException {
			m_stream.close();
			try {
				upload();
			} catch (final Exception ex) {
				throw new IOException(ex.getMessage());
			}
		}

		private void upload() throws Exception {
			final String bucketName = m_file.getContainerName();
			final String key = m_file.getBlobName();
			final Upload upload = m_file.getTransferManager().upload(bucketName, key, m_file.getTempFile());
			upload.waitForCompletion();
			m_file.getTempFile().delete();
			m_file.resetCache();
		}
	}
}
