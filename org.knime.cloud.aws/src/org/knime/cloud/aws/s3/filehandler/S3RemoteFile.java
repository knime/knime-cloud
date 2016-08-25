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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
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
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3RemoteFile extends RemoteFile<S3Connection> {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(S3RemoteFile.class);

	private static final String DELIMITER = "/";

	private String m_bucketNameCache = null;
	private String m_keyCache = null;
	private String m_fullPathCache = null;
	private Boolean m_existsCache = null;
	private Boolean m_isDirCache = null;
	private Long m_lastModifiedCache = null;
	private Long m_sizeCache = null;
	private Boolean m_isBucketCache = null;
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
			m_bucketNameCache = summary.getBucketName();
			m_keyCache = summary.getKey();
			m_lastModifiedCache = summary.getLastModified().getTime();
			m_sizeCache = summary.getSize();
		}
	}

	private S3Connection getOpenedConnection() throws Exception {
		open();
		return getConnection();
	}

	private AmazonS3Client getClient() throws Exception {
		return getOpenedConnection().getClient();
	}

	private TransferManager getTransferManager() throws Exception {
		return getOpenedConnection().getTransferManager();
	}

	@Override
	protected boolean usesConnection() {
		return true;
	}

	@Override
	protected S3Connection createConnection() {
		return new S3Connection(getConnectionInformation());
	}

	@Override
	public String getType() {
		return super.getConnectionInformation().getProtocol();
	}

	@Override
	public boolean exists() throws Exception {

		if (m_existsCache == null) {
			if (StringUtils.isBlank(getFullPathCache()) || getFullPathCache().equals(DELIMITER)) {
				// root folder
				m_existsCache = true;
			} else {
				final String bucketName = getBucketName();
				final boolean bucketExist = getOpenedConnection().isOwnBucket(bucketName);
				if (!bucketExist) {
					m_existsCache = false;
				} else if (isBucket()) {
					m_existsCache = true;
				} else {
					final String key = getKey();
					if (getClient().doesObjectExist(bucketName, key)) {
						m_existsCache = true;
					} else {
						m_existsCache = false;
					}
				}
			}
		}
		return m_existsCache;
	}

	@Override
	public boolean isDirectory() throws Exception {
		if (m_isDirCache == null) {
			final String path = getFullPathCache();

			if (StringUtils.isBlank(path) || path.endsWith(DELIMITER) || isBucket()) {
				// Root folder && directory folder
				m_isDirCache = true;
			} else {
				m_isDirCache = false;
			}
		}
		return m_isDirCache;

	}

	@Override
	public InputStream openInputStream() throws Exception {
		final String bucketName = getBucketName();
		final String key = getKey();
		final Download download = getTransferManager().download(bucketName, key, getTempFile());
		download.waitForCompletion();
		return new BufferedInputStream(new FileInputStream(getTempFile()));
	}

	@Override
	public OutputStream openOutputStream() throws Exception {
		return new S3OutputStream(this);
	}

	@Override
	public long getSize() throws Exception {
		if (m_sizeCache == null) {
			if (exists()) {
				m_sizeCache = 0l;
				if (isDirectory()) {
					for (final S3RemoteFile file : listFiles()) {
						m_sizeCache += file.getSize();
					}
				} else {
					final String bucketName = getBucketName();
					final String key = getKey();
					m_sizeCache = getClient().getObjectMetadata(bucketName, key).getContentLength();
				}
			} else {
				m_sizeCache = 0l;
			}
		}
		return m_sizeCache;
	}

	@Override
	public long lastModified() throws Exception {
		if (m_lastModifiedCache == null) {
			if (exists()) {
				m_lastModifiedCache = 0l;
				if (isDirectory()) {
					for (final S3RemoteFile file : listFiles()) {
						final long lastModified = file.lastModified();
						if (lastModified > m_lastModifiedCache) {
							m_lastModifiedCache = lastModified;
						}
					}
				} else {
					final String bucketName = getBucketName();
					final String key = getKey();
					m_lastModifiedCache = getClient().getObjectMetadata(bucketName, key).getLastModified().getTime();
				}
			} else {
				m_lastModifiedCache = 0l;
			}
		}
		return m_lastModifiedCache;
	}

	@Override
	public boolean delete() throws Exception {
		// Delete can only be true if the file exists
		boolean result = exists();
		if (exists()) {
			final String path = getFullPathCache();
			/* only delete if it is not a root directory */
			if (!StringUtils.isBlank(path)) {
				final String bucketName = getBucketName();
				try {
					getOpenedConnection().validateBucketName(bucketName);
					final String key = getKey();
					if (isDirectory()) {
						// final S3RemoteFile[] files = listFiles();
						for (final S3RemoteFile file : listFiles()) {
							file.delete();
						}
						if (isBucket()) {
							LOGGER.debug("Delete the bucket \"" + bucketName + "\"");
							getClient().deleteBucket(bucketName);
						} else {
							LOGGER.debug("Delete the directory \"" + key + "\" in the bucket \"" + bucketName + "\"");
							getClient().deleteObject(bucketName, key);
						}
					} else {
						LOGGER.debug("Delete the file \"" + key + "\" in the bucket \"" + bucketName + "\"");
						getClient().deleteObject(bucketName, key);
					}
					resetCache();
					result = result && !exists();
				} catch (final Exception ex) {
					result = false;
					LOGGER.debug(ex.getMessage());
				}
			}
		}

		return result;
	}

	@Override
	public S3RemoteFile[] listFiles() throws Exception {
		final S3RemoteFile[] files;
		// final String path = getURI().getPath();
		final String path = getFullPathCache();
		if (StringUtils.isBlank(path) || path.equals(DELIMITER)) {
			// Path is blank, assumed it is root directory, so fetch all buckets
			final List<Bucket> buckets = getClient().listBuckets();
			if (buckets == null || buckets.isEmpty()) {
				return new S3RemoteFile[0];
			}
			files = new S3RemoteFile[buckets.size()];
			for (int i = 0; i < files.length; i++) {
				final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
						getURI().getPort(), createBucketPath(buckets.get(i).getName()), getURI().getQuery(),
						getURI().getFragment());
				files[i] = new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor());
			}
		} else {
			final String bucketName = getBucketName();
			final String prefix = getKey();
			final ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
					.withPrefix(prefix).withDelimiter(DELIMITER);
			ListObjectsV2Result result;
			final List<S3RemoteFile> fileList = new ArrayList<S3RemoteFile>();
			do {
				result = getClient().listObjectsV2(request);
				for (final S3ObjectSummary summary : result.getObjectSummaries()) {
					if (!summary.getKey().equals(prefix)) {
						final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
								getURI().getPort(), createBucketPath(bucketName) + summary.getKey(),
								getURI().getQuery(), getURI().getFragment());
						fileList.add(
								new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor(), summary));
					}
				}

				for (final String commPrefix : result.getCommonPrefixes()) {
					final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
							getURI().getPort(), createBucketPath(bucketName) + commPrefix, getURI().getQuery(),
							getURI().getFragment());
					fileList.add(new S3RemoteFile(uri, getConnectionInformation(), getConnectionMonitor()));
				}

				request.setContinuationToken(result.getNextContinuationToken());
			} while (result.isTruncated());

			files = fileList.toArray(new S3RemoteFile[fileList.size()]);
			// Arrays.sort(files);
		}

		return files;
	}

	@Override
	public boolean mkDir() throws Exception {
		boolean result = false;

		try {
			// Get bucket name and folder name
			final String path = getFullPathCache();
			final String bucketName = getBucketName();
			// String folderName;
			if (isBucket()) {
				if (!getClient().doesBucketExist(bucketName)) {
					LOGGER.info("Create a new bucket \"" + bucketName + "\"");
					getClient().createBucket(bucketName);
					result = true;
				}
			} else {
				String dirName = getKey();
				dirName = (dirName.endsWith(DELIMITER)) ? dirName : dirName + DELIMITER;

				if (!getClient().doesObjectExist(bucketName, dirName)) {
					// Create meta data for folder and set content length to 0
					final ObjectMetadata metadata = new ObjectMetadata();
					metadata.setContentLength(0);

					// Create empty content
					final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
					// Create a PutObjectRequest passing the folder name
					// suffixed by the DELIMITER
					final PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, dirName, emptyContent,
							metadata);
					LOGGER.info("Create a new directory \"" + dirName + " in the bucket \"" + bucketName + "\"");
					// Send request to S3 to create folder
					getClient().putObject(putObjectRequest);
					result = true;
				}
			}

			resetCache();

			m_isDirCache = true;
			m_fullPathCache = (path.endsWith(DELIMITER)) ? path : path + DELIMITER;

		} catch (final Exception ex) {
			// result stays false
			LOGGER.debug(ex.getMessage());
		}

		return result;
	}

	@Override
	public RemoteFile<S3Connection> getParent() throws Exception {
		String path = getS3FullName();
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		path = FilenameUtils.getFullPath(path);
		// Build URI
		final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), getURI().getPort(),
				path, getURI().getQuery(), getURI().getFragment());
		// Create remote file and open it
		final RemoteFile<S3Connection> file = RemoteFileFactory.createRemoteFile(uri, getConnectionInformation(),
				getConnectionMonitor());
		file.open();
		return file;
	}

	private String getS3FullName() throws Exception {
		String fullname = getPath();
		if (!isDirectory()) {
			// Append name to path
			fullname += getName();
		}
		return fullname;
	}

	public String getBucketName() throws Exception {
		if (StringUtils.isBlank(m_bucketNameCache)) {
			final String path = getFullPathCache();
			final int idx = path.indexOf(DELIMITER);
			if (idx != 0) {
				throw new InvalidSettingsException("Invalid path. Root folder must begin with '/'");
				// m_bucketNameCache = null;
			} else {
				final int nextDelimiterIdx = path.indexOf(DELIMITER, idx + 1);
				if (nextDelimiterIdx < 0) {
					m_bucketNameCache = path.substring(idx + 1);
				} else {
					m_bucketNameCache = path.substring(idx + 1, nextDelimiterIdx);
				}
			}
			return m_bucketNameCache;
		}
		return m_bucketNameCache;
	}

	private String createBucketPath(final String bucketName) {
		return DELIMITER + bucketName + DELIMITER;
	}

	public String getKey() throws Exception {
		if (StringUtils.isBlank(m_keyCache)) {
			final String bucketName = getBucketName();
			m_keyCache = null;
			if (!isBucket()) {
				m_keyCache = getFullPathCache().substring(createBucketPath(bucketName).length());
			}
		}
		return m_keyCache;
	}

	private void resetCache() throws Exception {
		m_bucketNameCache = null;
		m_keyCache = null;
		m_isBucketCache = null;
		m_fullPathCache = null;
		m_existsCache = null;
		m_isDirCache = null;
		getOpenedConnection().resetCache();
	}

	private String getFullPathCache() throws Exception {
		if (m_fullPathCache == null) {
			m_fullPathCache = getURI().getPath();
		}

		return m_fullPathCache;
	}

	private boolean isBucket() throws Exception {
		if (m_isBucketCache == null) {
			final String bucketName = getBucketName();
			final String bucketPath = createBucketPath(bucketName);
			final String path = getFullPathCache().endsWith(DELIMITER) ? getFullPathCache()
					: getFullPathCache() + DELIMITER;
			if (bucketPath.equals(path)) {
				m_isBucketCache = true;
			} else {
				m_isBucketCache = false;
			}
		}
		return m_isBucketCache;
	}

	private File getTempFile() throws Exception {
		if (m_tmpFileCache == null) {
			m_tmpFileCache = File.createTempFile("s3tmpfile", null);
			m_tmpFileCache.deleteOnExit();
		}
		return m_tmpFileCache;
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
			final String bucketName = m_file.getBucketName();
			final String key = m_file.getKey();
			final Upload upload = m_file.getTransferManager().upload(bucketName, key, m_file.getTempFile());
			upload.waitForCompletion();
			m_file.getTempFile().delete();
			m_file.resetCache();
		}
	}

}
