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
 *   Aug 11, 2016 (oole): created
 */
package org.knime.cloud.azure.abs.filehandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobInputStream;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * Implementation of {@link CloudRemoteFile} for Azure Blob Store
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSRemoteFile extends CloudRemoteFile<AzureBSConnection> {

	/**
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 * @throws Exception
	 */
	public AzureBSRemoteFile(URI uri, final CloudConnectionInformation connectionInformation,
			ConnectionMonitor<AzureBSConnection> connectionMonitor) throws Exception {
		this(uri, connectionInformation, connectionMonitor, null);
	}

	/**
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 * @param blob
	 * @throws Exception
	 */
	public AzureBSRemoteFile(URI uri, final CloudConnectionInformation connectionInformation,
			ConnectionMonitor<AzureBSConnection> connectionMonitor, CloudBlockBlob blob) throws Exception {
		super(uri, connectionInformation, connectionMonitor);
		CheckUtils.checkArgumentNotNull(connectionInformation, "Connection Information mus not be null");
		if (blob != null) {
			try {
				m_containerName = blob.getContainer().getName();
			} catch (StorageException | URISyntaxException e) {
				throw new InvalidSettingsException("Somethings is wrong with the given blob");
			}
			m_blobName = blob.getName();
			m_lastModified = blob.getProperties().getLastModified().getTime();
			m_size = blob.getProperties().getLength();
		}
	}

	private CloudBlobClient getClient() throws Exception {
		return getOpenedConnection().getClient();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected AzureBSConnection createConnection() {
		return new AzureBSConnection((CloudConnectionInformation)getConnectionInformation());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doesContainerExist(String containerName) throws Exception {
		return getClient().getContainerReference(containerName).exists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doestBlobExist(String containerName, String blobName) throws Exception {
		return getClient().getContainerReference(containerName).getBlockBlobReference(blobName).exists();
	}

	@Override
	protected AzureBSRemoteFile[] listRootFiles() throws Exception {
		final Iterable<CloudBlobContainer> containers = getClient().listContainers();
		final Iterator<CloudBlobContainer> iterator = containers.iterator();
		final List<CloudBlobContainer> containerList = new ArrayList<>();
		iterator.forEachRemaining(containerList::add);
		final AzureBSRemoteFile[] files = new AzureBSRemoteFile[containerList.size()];
		for (int i = 0; i < files.length; i++) {
			final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
					getURI().getPort(), createContainerPath(containerList.get(i).getName()), getURI().getQuery(),
					getURI().getFragment());
			files[i] = new AzureBSRemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
					getConnectionMonitor());
		}
		return files;
	}

	@Override
	protected AzureBSRemoteFile[] listDirectoryFiles() throws Exception {
		final String containerName = getContainerName();
		final String prefix = getBlobName();
		final Iterable<ListBlobItem> blobs = getClient().getContainerReference(containerName).listBlobs(prefix, false);
		final Iterator<ListBlobItem> iterator = blobs.iterator();

		final List<AzureBSRemoteFile> fileList = new ArrayList<AzureBSRemoteFile>();
		while (iterator.hasNext()) {
			final ListBlobItem listBlobItem = iterator.next();
			if (listBlobItem instanceof CloudBlockBlob) {
				final CloudBlockBlob blob = (CloudBlockBlob) listBlobItem;
				if (!blob.getName().equals(prefix)) {
					final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
							getURI().getPort(), createContainerPath(containerName) + blob.getName(),
							getURI().getQuery(), getURI().getFragment());
					fileList.add(new AzureBSRemoteFile(uri,(CloudConnectionInformation) getConnectionInformation(), getConnectionMonitor(), blob));
				}
			} else if (listBlobItem instanceof CloudBlobDirectory) {
				final CloudBlobDirectory blobDir = (CloudBlobDirectory) listBlobItem;
				final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
						getURI().getPort(), createContainerPath(containerName) + blobDir.getPrefix(),
						getURI().getQuery(), getURI().getFragment());
				fileList.add(new AzureBSRemoteFile(uri,(CloudConnectionInformation) getConnectionInformation(), getConnectionMonitor()));
			}
		}
		final AzureBSRemoteFile[] files = fileList.toArray(new AzureBSRemoteFile[fileList.size()]);
		return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getBlobSize() throws Exception {
		return getClient().getContainerReference(getContainerName()).getBlobReferenceFromServer(getBlobName()).getProperties().getLength();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getLastModified() throws Exception {
		return getClient().getContainerReference(getContainerName()).getBlobReferenceFromServer(getBlobName()).getProperties().getLastModified().getTime();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteContainer() throws Exception {
		return getClient().getContainerReference(getContainerName()).deleteIfExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteDirectory() throws Exception {
		boolean result = exists();
		final CloudBlobDirectory directoryReference = getClient().getContainerReference(getContainerName()).getDirectoryReference(getBlobName());
		final Iterable<ListBlobItem> listBlobs = directoryReference.listBlobs();
		final Iterator<ListBlobItem> iterator = listBlobs.iterator();
		while (iterator.hasNext()) {
			final ListBlobItem next = iterator.next();
			final CloudBlockBlob blob = (CloudBlockBlob) next;
			result = blob.deleteIfExists();
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteBlob() throws Exception {
		return getClient().getContainerReference(getContainerName()).getBlockBlobReference(getBlobName()).deleteIfExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createContainer() throws Exception {
		return getClient().getContainerReference(getContainerName()).createIfNotExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createDirectory(final String dirName) throws Exception {
		final CloudBlockBlob dir = getClient().getContainerReference(getContainerName()).getBlockBlobReference(dirName);
		final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		dir.upload(emptyContent, 0l);
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputStream openInputStream() throws Exception {
		final BlobInputStream blobInputStream = getClient().getContainerReference(getContainerName()).getBlockBlobReference(getBlobName()).openInputStream();
		return blobInputStream;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputStream openOutputStream() throws Exception {
		final BlobOutputStream blobOutputStream = getClient().getContainerReference(getContainerName()).getBlockBlobReference(getBlobName()).openOutputStream();
		return blobOutputStream;
	}

    @Override
    public URI getHadoopFilesystemURI() throws Exception {
        // wasb(s)://<containername>@<accountname>.blob.core.windows.net/path
        final CloudConnectionInformation connectionInfo = (CloudConnectionInformation) getConnectionInformation();
        final String scheme = "wasb";
        final String account = connectionInfo.getUser();
        final String container = getContainerName();
        final String host = account + ".blob.core.windows.net";
        return new URI(scheme, container, host, -1, DELIMITER + getBlobName(), null, null);
    }
}
