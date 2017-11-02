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

import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSConnection extends Connection {

	public static final String PREFIX = "abs";
	public static final String HOST = "blob.core.windows.net";
	public static final int PORT = -1;
	private static final NodeLogger LOGGER = NodeLogger.getLogger(AzureBSConnection.class);

	private final CloudConnectionInformation m_connectionInformation;

	private CloudBlobClient m_client;

	/**
	 * Constructor.
	 */
	public AzureBSConnection(final CloudConnectionInformation connectionInformation) {
		m_connectionInformation = connectionInformation;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open() throws Exception {
		if (!isOpen()) {
			final String storageAccount = m_connectionInformation.getUser();
			final String accessKey = KnimeEncryption.decrypt(m_connectionInformation.getPassword());

			LOGGER.info("Create a new CloudBlobClient with connection timeout " + m_connectionInformation.getTimeout() + " milliseconds");
			final StorageCredentialsAccountAndKey storageCredentials = new StorageCredentialsAccountAndKey(storageAccount, accessKey);
			final CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentials, true);
			m_client = cloudStorageAccount.createCloudBlobClient();
			m_client.getDefaultRequestOptions().setTimeoutIntervalInMs(m_connectionInformation.getTimeout());

			try {
				m_client.downloadServiceProperties();
			} catch (final StorageException e) {
				if (e.getCause() instanceof UnknownHostException) {
					throw new InvalidSettingsException("Unable to connect. Check credentials.");
				} else {
					throw new InvalidSettingsException(e.getErrorCode() + ". Check credentials");
				}

			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {
		if (m_client != null) {
			return true;
		}
		return false;
	}

	/**
	 * Returns the {@link CloudBlobClient} for this connection
	 *
	 * @return the {@link CloudBlobClient} for this connection
	 */
	public CloudBlobClient getClient() {
		return m_client;
	}

	/**
	 * Check whether a blob with the given name exists in the given container
	 *
	 * @param containerName the given containerName
	 * @param name the given blob name
	 * @return <code>true</code> if the blob exists, or <code>false</code> otherwise.
	 * @throws StorageException
	 * @throws URISyntaxException
	 */
	public boolean doesBlobExist(String containerName, String name) throws URISyntaxException, StorageException {
		final CloudBlobContainer containerReference = m_client.getContainerReference(containerName);
		final CloudBlockBlob blockBlobReference = containerReference.getBlockBlobReference(name);
		return blockBlobReference.exists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws Exception {
		// nothing to do
	}
}
