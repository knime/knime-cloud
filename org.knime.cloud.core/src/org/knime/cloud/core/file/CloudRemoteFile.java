/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
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
package org.knime.cloud.core.file;

import java.net.URI;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

/**
 * Abstract class performing path resolution for cloud storage system with virtual filesystems
 * (Azure, S3, Google Storage, ...)
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public abstract class CloudRemoteFile<C extends Connection> extends RemoteFile<C> {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(CloudRemoteFile.class);

	protected static final String DELIMITER = "/";

	protected String m_fullPath = null;
	protected String m_containerName = null;
	protected String m_blobName = null;
	protected Boolean m_isContainer = null;
	protected Boolean m_exists = null;
	protected Boolean m_isDir = null;
	protected Long m_lastModified = null;
	protected Long m_size = null;

	/**
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 */
	protected CloudRemoteFile(URI uri, ConnectionInformation connectionInformation,
			ConnectionMonitor<C> connectionMonitor) {
		super(uri, connectionInformation, connectionMonitor);
	}

	/**
	 * Whether a container with the given name exists
	 * @param containerName the name of the container that should be checked
	 * @return <code>true</code> if the container exists, <code>false</code> otherwise
	 * @throws Exception
	 */
	protected abstract boolean doesContainerExist(final String containerName) throws Exception;

	/**
	 * Whether a blob/file with the given name exists
	 * @param containerName the name of the container in which the blob/file resides
	 * @param blobName	the name of the file/blob to be checked
	 * @return <code>true</code> if the blob/file exists, <code>false</code> otherwise
	 * @throws Exception
	 */
	protected abstract boolean doestBlobExist(final String containerName, final String blobName) throws Exception;

	/**
	 * A list of remote files on from root level including containers
	 * @return An array of files in the given storage
	 * @throws Exception
	 */
	protected abstract CloudRemoteFile<C>[] listRootFiles() throws Exception;

	/**
	 * List Remote Files in this {@link RemoteFile}'s directory
	 * @return An array of files in this directory
	 * @throws Exception
	 */
	protected abstract CloudRemoteFile<C>[] listDirectoryFiles() throws Exception;

	/**
	 * Get this blob/file's size in byte
	 * @return this blob/file's size
	 * @throws Exception
	 */
	protected abstract long getBlobSize() throws Exception;

	/**
	 * Returns when this blob/file was last modified
	 * @return when this blob/file was last modified
	 * @throws Exception
	 */
	protected abstract long getLastModified() throws Exception;

	/**
	 * Deletes this RemoteFile's container
	 * @return <code>true</code> if the container is deleted
	 * @throws Exception
	 */
	protected abstract boolean deleteContainer() throws Exception;

	/**
	 * Deletes this RemoteFile's directory
	 * @return <code>true</code> if the directory is deleted
	 * @throws Exception
	 */
	protected abstract boolean deleteDirectory() throws Exception;

	/**
	 * Deletes this blob/file
	 * @return <code>true</code> if the blob/file is deleted
	 * @throws Exception
	 */
	protected abstract boolean deleteBlob() throws Exception;

	/**
	 * Creates a container with this RemoteFile's container name
	 * @return true if the container is created
	 * @throws Exception
	 */
	protected abstract boolean createContainer() throws Exception;

	/**
	 * Creates a directory with the given directory name
	 * @param dirName the directory name
	 * @return <code>true</code> if the directory is created
	 * @throws Exception
	 */
	protected abstract boolean createDirectory(final String dirName) throws Exception;

	/**
	 * Needs to create the {@link CloudRemoteFile}'s specific connection
	 * {@inheritDoc}
	 */
	@Override
	abstract protected C createConnection();

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
				final boolean containerExists = doesContainerExist(containerName);
				if (!containerExists) {
					m_exists = false;
				} else if (isContainer()){
					m_exists = true;
				} else {
					final String name = getBlobName();
					if (doestBlobExist(containerName, name)) {
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
	protected String createContainerPath(String containerName) {
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
	public RemoteFile<C> getParent() throws Exception {
		String path = getABSFullName();
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		path = FilenameUtils.getFullPath(path);
		// Build URI
		final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), getURI().getPort(),
				path, getURI().getQuery(), getURI().getFragment());
		// Create remote file and open it
		final RemoteFile<C> file = RemoteFileFactory.createRemoteFile(uri, getConnectionInformation(),
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
	public long getSize() throws Exception {
		if (m_size == null) {
			if (exists()) {
				m_size = 0l;
				if (isDirectory()) {
					for (final CloudRemoteFile<C> file : listFiles()) {
						m_size += file.getSize();
					}
				} else {
					// blob reference must be retrieved from the server otherwise properties will return null
					m_size = getBlobSize();
				}
			} else {
				m_size = 0l;
			}
		}
		return m_size;
	}

	@Override
	public CloudRemoteFile<C>[] listFiles() throws Exception {
		final CloudRemoteFile<C>[] files;

		final String path =getFullPath();
		if (StringUtils.isBlank(path) || path.equals(DELIMITER)	) {
			files = listRootFiles();
		} else {
			files = listDirectoryFiles();
		}

		return files;
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
					for (final CloudRemoteFile<C> file : listFiles()) {
						final long lastModified = file.lastModified();
						if (lastModified > m_lastModified) {
							m_lastModified= file.lastModified();
						}
					}
				} else {
					// Blob reference must be recieved directly from server. Otherwise properties are null.
					m_lastModified = getLastModified();
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
						result = deleteContainer();
					} else {
						result = deleteDirectory();
						LOGGER.debug("Delete the directory \"" + blobName + "\" in container \"" + containerName + "\"");
					}
				} else {
					LOGGER.debug("Delete the file \"" + blobName + "\" in container\"" + containerName + "\"");
					result = deleteBlob();
				}
				resetCache();
				result = result && !exists();
			} catch (final Exception ex) {
				result = false;
				LOGGER.debug(ex.getMessage());
				throw ex;
			}
		}
		return result;
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
					result = createContainer();
			} else {
				String dirName = getBlobName();
				dirName = (dirName.endsWith(DELIMITER)) ? dirName : dirName + DELIMITER;
				if (!doestBlobExist(containerName, dirName)) {
					LOGGER.info("Create a new directory \"" + dirName + "\" in the container \"" + containerName + "\"");
					result = createDirectory(dirName);
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
	 * @throws Exception
	 */
	private boolean isContainer() throws Exception {
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
	 * Returns this file's connection
	 * @return this file's connection
	 * @throws Exception
	 */
	protected C getOpenedConnection() throws Exception {
		open();
		return getConnection();
	}

	/**
	 * Reset this file's cached attributes
	 * @throws Exception if something is wrong with the client's configuration
	 */
	protected void resetCache() throws Exception {
		m_containerName = null;
		m_blobName = null;
		m_isContainer = null;
		m_fullPath = null;
		m_exists = null;
		m_isDir = null;
	}

	/**
	 * Creates an Hadoop Filesystem URI to access the given file via the Hadoop
	 * Filesystem APIs. Implementations of this method may throw
	 * java.lang.UnsupportedOperationException to indicate that the creation of
	 * such an URI is not supported.
	 *
	 * @throws java.lang.UnsupportedOperationException
	 *             if creation of such an URI is not supported.
	 * @return Hadoop Filesystem URI
	 */
	public abstract URI getHadoopFilesystemURI() throws Exception;
}
