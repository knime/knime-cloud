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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.util.CheckUtils;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;

/**
 * Implementation of {@link CloudRemoteFile} for Amazon S3
 *
 * @author Budi Yanto, KNIME.com
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class S3RemoteFile extends CloudRemoteFile<S3Connection> {

	protected S3RemoteFile(final URI uri, final CloudConnectionInformation connectionInformation,
			final ConnectionMonitor<S3Connection> connectionMonitor) {
		this(uri, connectionInformation, connectionMonitor, null);
	}

	protected S3RemoteFile(final URI uri, final CloudConnectionInformation connectionInformation,
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
		return new S3Connection((CloudConnectionInformation)getConnectionInformation());
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
	protected boolean doesContainerExist(final String containerName) throws Exception {
		return getOpenedConnection().isOwnBucket(containerName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doestBlobExist(final String containerName, final String blobName) throws Exception {
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
			files[i] = new S3RemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
					getConnectionMonitor());
		}
		return files;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    protected S3RemoteFile[] listDirectoryFiles() throws Exception {
        S3RemoteFile[] files;
        try {
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
                        fileList.add(new S3RemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
                                getConnectionMonitor(), summary));
                    }
                }

                for (final String commPrefix : result.getCommonPrefixes()) {
                    final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
                            getURI().getPort(), createContainerPath(bucketName) + commPrefix, getURI().getQuery(),
                            getURI().getFragment());
                    fileList.add(new S3RemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
                            getConnectionMonitor()));
                }

                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

            files = fileList.toArray(new S3RemoteFile[fileList.size()]);
            // Arrays.sort(files);

        } catch (AmazonS3Exception e){
            // Listing does not work, when bucket is in wrong region, return empty array of files -- see AP-6662
            files = new S3RemoteFile[0];
        }
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
	protected boolean createDirectory(final String dirName) throws Exception {
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

		final GetObjectRequest getRequest = new GetObjectRequest(bucketName, key);
		// create input stream from the S3Object specified in the getRequest
		final S3Object object = getClient().getObject(getRequest);
		final S3ObjectInputStream s3inputStream = object.getObjectContent();

		return new BufferedInputStream(s3inputStream);
	}

	/**
	 * {@inheritDoc}
	 * This must not be used. The {@link #write(RemoteFile, ExecutionContext) write} is overwritten.
	 */
	@Override
	public OutputStream openOutputStream() throws Exception {
		throw new UnsupportedOperationException("openOutputStream must not be used for S3 connections.");
	}

	/**
     * Write the given remote file into this files location.
     *
     *
     * This method will overwrite the old file if it exists.
     *
     * @param file Source remote file
     * @param exec Execution context for <code>checkCanceled()</code> and
     *            <code>setProgress()</code>
     * @throws Exception If the operation could not be executed
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void write(final RemoteFile file, final ExecutionContext exec) throws Exception {
        // Default implementation using just remote file methods
        try (final InputStream in = file.openInputStream()){
            final String uri = getURI().toString();
            final ObjectMetadata metadata = new ObjectMetadata();
            long fileSize = file.getSize();
            metadata.setContentLength(fileSize);
            final PutObjectRequest putRequest = new PutObjectRequest(getContainerName(), getBlobName(), in, metadata);
            Upload upload = getTransferManager().upload(putRequest);

            S3ProgressListener progressListener = new S3ProgressListener() {

                long totalTransferred = 0;

                @Override
                public void progressChanged(final ProgressEvent progressEvent) {
                    totalTransferred+=progressEvent.getBytesTransferred();
                    double percent = totalTransferred/(fileSize/100);
                    exec.setProgress(percent / 100, () -> "Written: " + FileUtils.byteCountToDisplaySize(totalTransferred) + " to file " + uri);
                }

                @Override
                public void onPersistableTransfer(final PersistableTransfer persistableTransfer) {
                    // Not used since we are not going to pause/unpause upload
                }
            };

            upload.addProgressListener(progressListener);
            upload.waitForCompletion();
        } catch (InterruptedException e) {
            // removes uploaded parts of failed uploads on given bucket uploaded before now
            final Date now = new Date (System.currentTimeMillis());
            getTransferManager().abortMultipartUploads(getContainerName(), now);
            // check if canceled, otherwise throw exception
            exec.checkCanceled();
            throw e;
        }
    }


	@Override
	protected void resetCache() throws Exception {
		super.resetCache();
		getOpenedConnection().resetCache();
	}

    @Override
    public URI getHadoopFilesystemURI() throws Exception {
        // s3://<containername>/<path>
        return new URI("s3", null, getContainerName(), -1, DELIMITER + getBlobName(), null, null);
    }
}
