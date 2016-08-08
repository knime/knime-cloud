package org.knime.cloud.aws.s3.filehandler;

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;

import com.amazonaws.services.s3.AmazonS3;

/**
 * S3 remote file handler
 *
 * @author Budi Yanto, KNIME.com, Berlin, Germany
 *
 */
public class S3RemoteFileHandler implements RemoteFileHandler<S3Connection> {

	public static final String DEFAULT_REGION = "s3.amazonaws.com";

	/** The {@link Protocol} of this {@link RemoteFileHandler}. */
	public static final Protocol PROTOCOL = new Protocol(AmazonS3.ENDPOINT_PREFIX, -1, false, false, false, true, true,
			true, true, false);

	@Override
	public Protocol[] getSupportedProtocols() {
		return new Protocol[] { PROTOCOL };
	}

	@Override
	public RemoteFile<S3Connection> createRemoteFile(final URI uri, final ConnectionInformation connectionInformation,
			final ConnectionMonitor<S3Connection> connectionMonitor) throws Exception {
		final S3RemoteFile remoteFile = new S3RemoteFile(uri, connectionInformation, connectionMonitor);
		return remoteFile;
	}

}
