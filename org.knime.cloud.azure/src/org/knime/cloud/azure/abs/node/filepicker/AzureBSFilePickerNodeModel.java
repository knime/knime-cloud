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
package org.knime.cloud.azure.abs.node.filepicker;

import java.net.URI;
import java.util.Date;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.cloud.azure.abs.filehandler.AzureBSRemoteFile;
import org.knime.cloud.core.node.filepicker.AbstractFilePickerNodeModel;
import org.knime.core.node.NodeLogger;

import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

/**
 *	Node model for the Azure Blob Store Connection
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSFilePickerNodeModel extends AbstractFilePickerNodeModel {

	private static NodeLogger LOGGER = NodeLogger.getLogger(AzureBSFilePickerNodeModel.class);

	static final String CFG_FILE_SELECTION = "fileSelection";

	private static final String FLOW_VARIABLE_NAME = "AzurePickedFile";


	/**
	 * @param nrInDataPorts
	 * @param nrOutDataPorts
	 */
	protected AzureBSFilePickerNodeModel() {
		super(CFG_FILE_SELECTION,FLOW_VARIABLE_NAME);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getSignedURL(final ConnectionMonitor<? extends Connection> monitor, final ConnectionInformation connectionInformation) throws Exception {
		final URI uri = new URI(connectionInformation.toURI().toString() + NodeUtils.encodePath(getFileSelection()));

		final AzureBSRemoteFile remoteFile =
				(AzureBSRemoteFile) RemoteFileFactory.createRemoteFile(uri, connectionInformation, monitor);
		remoteFile.open();

		final CloudBlobClient client = remoteFile.getConnection().getClient();
		final String containerName = remoteFile.getContainerName();
		final String blobName = remoteFile.getBlobName();

		final Date expirationTime = getExpirationTime();

		LOGGER.debug("Generate Presigned URL with expiration time: " + expirationTime);

		// create sas url here

		final CloudBlockBlob blockBlobReference = client.getContainerReference(containerName).getBlockBlobReference(blobName);
		final SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
		sasConstraints.setSharedAccessExpiryTime(expirationTime);
		sasConstraints.setPermissionsFromString("r"); // complete permissions: racwdl

		final String sasToken = blockBlobReference.generateSharedAccessSignature(sasConstraints, null);
		final String sasUri = blockBlobReference.getUri() + "?" + sasToken;
		return sasUri;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getEndpointPrefix() {
		return AzureBSConnection.PREFIX;
	}

}
