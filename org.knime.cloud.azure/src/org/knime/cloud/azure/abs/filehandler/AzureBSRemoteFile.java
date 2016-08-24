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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
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
 * {@link RemoteFile} for the Azure Blob Store.
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSRemoteFile extends RemoteFile<AzureBSConnection> {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(AzureBSRemoteFile.class);

	private static final String DELIMITER = "/";

	private String m_fullPath = null;
	private String m_containerName = null;
	private String m_blobName = null;
	private Boolean m_isContainer = null;
	private Boolean m_exists = null;
	private Boolean m_isDir = null;
	private Long m_lastModified = null;
	private Long m_size = null;

	/**
	 * Constructor.
	 *
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 * @throws Exception
	 */
	public AzureBSRemoteFile(URI uri, ConnectionInformation connectionInformation,
			ConnectionMonitor<AzureBSConnection> connectionMonitor) throws Exception {
		this(uri, connectionInformation, connectionMonitor, null);
	}

	/**
	 * Contructor with given blob(reference) information.
	 *
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 * @param blob
	 * @throws InvalidSettingsException
	 */
	public AzureBSRemoteFile(URI uri, ConnectionInformation connectionInformation,
			ConnectionMonitor<AzureBSConnection> connectionMonitor, CloudBlockBlob blob) throws InvalidSettingsException {
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean usesConnection() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected AzureBSConnection createConnection() {
		return new AzureBSConnection(getConnectionInformation());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getType() {
		return super.getConnectionInformation().getProtocol();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists() throws Exception {
		if (m_exists == null) {
			if (StringUtils.isBlank(getFullPath()) || getFullPath().equals(DELIMITER)) {
				m_exists = true;
			} else {
				final String containerName = getContainerName();
				final boolean containerExists = getClient().getContainerReference(containerName).exists();
				if (!containerExists) {
					m_exists = false;
				} else if (isContainer()){
					m_exists = true;
				} else {
					final String name = getBlobName();
					if (getOpenedConnection().doesBlobExist(containerName, name)) {
						m_exists = true;
					} else {
						m_exists = false;
					}
				}
			}
		}

		return m_exists;
	}




	/**
	 * Returns this files blob name
	 *
	 * @return this files blob name
	 * @throws Exception if something is wrong with the client configuration
	 */
	public String getBlobName() throws Exception {
		if (m_blobName == null) {
			final String containerName = getContainerName();
			if (!isContainer()) {
				m_blobName = getFullPath().substring(createContainerPath(containerName).length());
			}
		}
		return m_blobName;
	}

	/**
	 * Creates the container path from a given container name
	 *
	 * @param containerName the container's name
	 * @return the container path
	 */
	private String createContainerPath(String containerName) {
		return DELIMITER + containerName + DELIMITER;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isDirectory() throws Exception {
		if (m_isDir == null) {
			final String path = getFullPath();

			if (StringUtils.isBlank(path) || path.endsWith(DELIMITER) || isContainer()) {
				m_isDir = true;
			} else {
				m_isDir = false;
			}
		}

		return m_isDir;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RemoteFile<AzureBSConnection> getParent() throws Exception {
		String path = getABSFullName();
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		path = FilenameUtils.getFullPath(path);
		// Build URI
		final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), getURI().getPort(),
				path, getURI().getQuery(), getURI().getFragment());
		// Create remote file and open it
		final RemoteFile<AzureBSConnection> file = RemoteFileFactory.createRemoteFile(uri, getConnectionInformation(),
				getConnectionMonitor());
		file.open();
		return file;
	}

	/**
	 * Returns the full name for the file in the amazon blob store
	 * @return the full name for the file in the amazon blob store
	 * @throws Exception If something is wrong with the client configuration
	 */
	private String getABSFullName() throws Exception {
		String fullname = getPath();
		if (!isDirectory()) {
			// Append name to path
			fullname += getName();
		}
		return fullname;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputStream openInputStream() throws Exception {
		final BlobInputStream blobInputStream = getBlobReference().openInputStream();
		return blobInputStream;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputStream openOutputStream() throws Exception {
		final BlobOutputStream blobOutputStream = getBlobReference().openOutputStream();
		return blobOutputStream;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getSize() throws Exception {
		if (m_size == null) {
			if (exists()) {
				m_size = 0l;
				if (isDirectory()) {
					for (final AzureBSRemoteFile file : listFiles()) {
						m_size += file.getSize();
					}
				} else {
					// blob reference must be retrieved from the server otherwise properties will return null
					m_size = getClient().getContainerReference(getContainerName()).getBlobReferenceFromServer(getBlobName()).getProperties().getLength();
				}
			} else {
				m_size = 0l;
			}
		}
		return m_size;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long lastModified() throws Exception {
		if (m_lastModified == null) {
			if (exists()) {
				m_lastModified = 0l;
				if (isDirectory()) {
					for (final AzureBSRemoteFile file : listFiles()) {
						final long lastModified = file.lastModified();
						if (lastModified > m_lastModified) {
							m_lastModified= file.lastModified();
						}
					}
				} else {
					// Blob reference must be recieved directly from server. Otherwise properties are null.
					m_lastModified = getClient().getContainerReference(getContainerName()).getBlobReferenceFromServer(getBlobName()).getProperties().getLastModified().getTime();
				}
			} else {
				m_lastModified = 0l;
			}
		}
		return m_lastModified;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean delete() throws Exception {
		boolean result = exists();
		final String path = getFullPath();
		if (!StringUtils.isBlank(path)) {
			final String containerName = getContainerName();
			final String blobName = getBlobName();
			try {
				if (isDirectory()) {
					if (isContainer()) {
						LOGGER.debug("Delete the container \"" + containerName + "\"");
						result = getClient().getContainerReference(containerName).deleteIfExists();
					} else {
						final CloudBlobDirectory directoryReference = getClient().getContainerReference(containerName).getDirectoryReference(blobName);
						final Iterable<ListBlobItem> listBlobs = directoryReference.listBlobs();
						final Iterator<ListBlobItem> iterator = listBlobs.iterator();
						while (iterator.hasNext()) {
							final ListBlobItem next = iterator.next();
							final CloudBlockBlob blob = (CloudBlockBlob) next;
							result = blob.deleteIfExists();
						}
						LOGGER.debug("Delete the directory \"" + blobName + "\" in container \"" + containerName + "\"");
					}
				} else {
					LOGGER.debug("Delete the file \"" + blobName + "\" in container\"" + containerName + "\"");
					result = getBlobReference().deleteIfExists();
				}
				resetCache();
				result = result && !exists();
			} catch (final Exception ex) {
				result = false;
				LOGGER.debug(ex.getMessage());
			}
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AzureBSRemoteFile[] listFiles() throws Exception {
		final AzureBSRemoteFile[] files;

		final String path = getFullPath();
		if (StringUtils.isBlank(path) || path.equals(DELIMITER)) {
			final Iterable<CloudBlobContainer> containers = getClient().listContainers();
			final Iterator<CloudBlobContainer> iterator = containers.iterator();
			final List<CloudBlobContainer> containerList = new ArrayList<>();
			iterator.forEachRemaining(containerList::add);
			files = new AzureBSRemoteFile[containerList.size()];
			for (int i = 0; i < files.length; i++) {
				final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
						getURI().getPort(), createContainerPath(containerList.get(i).getName()), getURI().getQuery(),
						getURI().getFragment());
				files[i] = new AzureBSRemoteFile(uri, getConnectionInformation(), getConnectionMonitor());
			}
		} else {
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
						fileList.add(new AzureBSRemoteFile(uri, getConnectionInformation(), getConnectionMonitor(), blob));
					}
				} else if (listBlobItem instanceof CloudBlobDirectory) {
					final CloudBlobDirectory blobDir = (CloudBlobDirectory) listBlobItem;
					final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
							getURI().getPort(), createContainerPath(containerName) + blobDir.getPrefix(),
							getURI().getQuery(), getURI().getFragment());
					fileList.add(new AzureBSRemoteFile(uri, getConnectionInformation(), getConnectionMonitor()));
				}
			}
			files = fileList.toArray(new AzureBSRemoteFile[fileList.size()]);
		}

		return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean mkDir() throws Exception {
		boolean result = false;

		try {
			final String path = getFullPath();
			final String containerName = getContainerName();

			if (isContainer()) {
					result = getClient().getContainerReference(containerName).createIfNotExists();
			} else {
				String dirName = getBlobName();
				dirName = (dirName.endsWith(DELIMITER)) ? dirName : dirName + DELIMITER;
				if (!getOpenedConnection().doesBlobExist(containerName, dirName)) {
					final CloudBlockBlob dir = getClient().getContainerReference(containerName).getBlockBlobReference(dirName);
					final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

					LOGGER.info("Create a new directory \"" + dirName + "\" in the bucket \"" + containerName + "\"");
					dir.upload(emptyContent, 0l);
					result = true;
				}
			}

			resetCache();
			m_isDir = true;
			m_fullPath = (path.endsWith(DELIMITER)) ? path : path + DELIMITER;

		} catch (final Exception e) {
			LOGGER.debug(e.getMessage());
			throw e;
		}

		return result;
	}


	/**
	 * Returns this file's connection's {@link CloudBlobClient}
	 * @return this file's connection's {@link CloudBlobClient}
	 * @throws Exception if something is wrong with the client's configuration
	 */
	private CloudBlobClient getClient() throws Exception {
		return getOpenedConnection().getClient();
	}

	/**
	 * Returns this file's connection
	 * @return this file's connection
	 * @throws Exception
	 */
	private AzureBSConnection getOpenedConnection() throws Exception {
		open();
		return getConnection();
	}

	/**
	 * Returns this files container name
	 * @return this files container name
	 * @throws Exception
	 */
	public String getContainerName() throws Exception{
		if (m_containerName == null) {
			final String path = getFullPath();
			final int idx = path.indexOf(DELIMITER);
			if (idx != 0) {
				throw new InvalidSettingsException("Invalid path. Path must begin with /");
			} {
				final int nextDelimiterIdx = path.indexOf(DELIMITER, idx +1);
				if (nextDelimiterIdx < 0) {
					m_containerName = path.substring(idx+1);
				} else {
					m_containerName = path.substring(idx+1, nextDelimiterIdx);
				}
			}
		}
		return m_containerName;
	}

	/**
	 * Get this file's full path
	 * @return this file's full path
	 */
	private String getFullPath() {
		if (m_fullPath == null) {
			m_fullPath = getURI().getPath();
		}

		return m_fullPath;
	}

	/**
	 * Returns whether this file represents a container or not
	 * @return <code>true</code> if this file represents a container, or <code>false</code> if it doesn't
	 * @throws URISyntaxException
	 * @throws StorageException
	 * @throws Exception
	 */
	private boolean isContainer() throws URISyntaxException, StorageException, Exception {
		if (m_isContainer == null) {
			final String containerName = getContainerName();
			final String containerPath = createContainerPath(containerName);
			final String path = getFullPath().endsWith(DELIMITER) ? getFullPath() : getFullPath() + DELIMITER;
			if (containerPath.equals(path)) {
				m_isContainer = true;
			} else {
				m_isContainer = false;
			}

		}
		return m_isContainer;
	}

	/**
	 * Reset this file's cached attributes
	 * @throws Exception if something is wrong with the client's configuration
	 */
	private void resetCache() throws Exception {
		m_containerName = null;
		m_blobName = null;
		m_isContainer = null;
		m_fullPath = null;
		m_exists = null;
		m_isDir = null;
	}

	/**
	 * Returns the reference to this remote file's {@link CloudBlockBlob}
	 * @return this remote file's CloudBlockBlob reference
	 * @throws URISyntaxException
	 * @throws StorageException
	 * @throws Exception
	 */
	private CloudBlockBlob getBlobReference() throws URISyntaxException, StorageException, Exception {
		return getClient().getContainerReference(getContainerName()).getBlockBlobReference(getBlobName());
	}
}
