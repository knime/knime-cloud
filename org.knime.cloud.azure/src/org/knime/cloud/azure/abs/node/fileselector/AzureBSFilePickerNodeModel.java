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
package org.knime.cloud.azure.abs.node.fileselector;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.cloud.azure.abs.filehandler.AzureBSRemoteFile;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelDate;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;

import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

/**
 *	Node model for the Azure Blob Store Connection
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSFilePickerNodeModel extends NodeModel {

	private static NodeLogger LOGGER = NodeLogger.getLogger(AzureBSFilePickerNodeModel.class);

	static final String CFG_EXPIRATION_TIME = "expirationTime";
	static final String CFG_FILE_SELECTION = "fileSelection";

	private static final long DEF_EXPIRATION_TIME = 60*60*1000;

	private static final String FLOW_VARIABLE_NAME = "AzurePickedFile";

	private final SettingsModelDate m_dateSettingsModel = createExpirationSettingsModel();
	private String m_fileSelection;
	private ConnectionInformation m_connectionInformation;

	static SettingsModelDate createExpirationSettingsModel() {
		final SettingsModelDate model = new SettingsModelDate(CFG_EXPIRATION_TIME);
		model.setTimeInMillis(new Date().getTime());
		return model;
	}

	/**
	 * @param nrInDataPorts
	 * @param nrOutDataPorts
	 */
	protected AzureBSFilePickerNodeModel() {
		super (new PortType[] {ConnectionInformationPortObject.TYPE}, new PortType[] {FlowVariablePortObject.TYPE});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
		final ConnectionMonitor<? extends Connection> monitor = new ConnectionMonitor<>();
		// TODO SAS - Signed Access Signatures
		final URI uri = new URI(m_connectionInformation.toURI().toString() + NodeUtils.encodePath(m_fileSelection));

		final AzureBSRemoteFile remoteFile =
				(AzureBSRemoteFile) RemoteFileFactory.createRemoteFile(uri, m_connectionInformation, monitor);
		remoteFile.open();

		final CloudBlobClient client = remoteFile.getConnection().getClient();
		final String containerName = remoteFile.getContainerName();
		final String blobName = remoteFile.getBlobName();

		final Date expirationTime = m_dateSettingsModel.getDate();

		if (expirationTime.before(new Date())) {
			expirationTime.setTime(new Date().getTime() + DEF_EXPIRATION_TIME);
		}

		LOGGER.debug("Generate Presigned URL with expiration time: " + expirationTime);


		// create sas url here

		final CloudBlockBlob blockBlobReference = client.getContainerReference(containerName).getBlockBlobReference(blobName);
		final SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
		sasConstraints.setSharedAccessExpiryTime(expirationTime);
		sasConstraints.setPermissionsFromString("r"); // complete permissions: racwdl

		final String sasToken = blockBlobReference.generateSharedAccessSignature(sasConstraints, null);
		final String sasUri = blockBlobReference.getUri() + "?" + sasToken;

		final Set<String> variables = getAvailableFlowVariables().keySet();
		String name = FLOW_VARIABLE_NAME;
		if (variables.contains(name)) {
			int i = 2;
			name += "_";
			while (variables.contains(name + i)) {
				i++;
			}
			name += i;
		}

		LOGGER.debug("Push flow variable: " + sasUri);

		pushFlowVariableString(name, sasUri);

		return new PortObject[] { FlowVariablePortObject.INSTANCE };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs[0] != null) {
			final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
			m_connectionInformation = object.getConnectionInformation();
			if (m_connectionInformation == null
					|| !m_connectionInformation.getProtocol().equals(AzureBSConnection.PREFIX)) {
				throw new InvalidSettingsException("No ABS connection information available");
			}
		} else {
			throw new InvalidSettingsException("No ABS connection information available");
		}

		if (StringUtils.isBlank(m_fileSelection) || !m_fileSelection.startsWith("/") || m_fileSelection.endsWith("/")
				|| m_fileSelection.indexOf("/", 1) < 0) {
			throw new InvalidSettingsException("The selected file is not valid");
		}

		if (m_dateSettingsModel.getDate().before(new Date())) {
			setWarningMessage(
					"The selected expiration time is before the current time, the default 1 hour expiration time will be used");
		}

		LOGGER.debug("Current Time Configure: " + new Date());

		return new PortObjectSpec[] { FlowVariablePortObjectSpec.INSTANCE };

	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// Nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// Nothing to do
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		settings.addString(CFG_FILE_SELECTION, m_fileSelection);
		m_dateSettingsModel.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		settings.getString(CFG_FILE_SELECTION);
		m_dateSettingsModel.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_fileSelection = settings.getString(CFG_FILE_SELECTION);
		m_dateSettingsModel.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// Nothing to do
	}
}
