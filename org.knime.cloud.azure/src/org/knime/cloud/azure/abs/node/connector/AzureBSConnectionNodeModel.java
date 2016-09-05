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
package org.knime.cloud.azure.abs.node.connector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.cloud.azure.abs.filehandler.AzureBSRemoteFileHandler;
import org.knime.cloud.azure.abs.util.AzureConnectionInformationPortObject;
import org.knime.cloud.azure.abs.util.AzureConnectionInformationSettings;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.util.Pair;

/**
 *	Node model for the Azure Blob Store Connection
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSConnectionNodeModel extends NodeModel {

	private final AzureConnectionInformationSettings m_model = createAzureBSSettings();

	static AzureConnectionInformationSettings createAzureBSSettings() {
		return new AzureConnectionInformationSettings(AzureBSConnection.PREFIX);
	}

	static HashMap<AuthenticationType, Pair<String, String>> getNameMap() {
		final HashMap<AuthenticationType, Pair<String, String>> nameMap = new HashMap<>();
		nameMap.put(AuthenticationType.USER_PWD, new Pair<String, String>("Storage Account and Access Key", "Storage Account and Access Key based authentication"));
		return nameMap;
	}
	/**
	 * @param nrInDataPorts
	 * @param nrOutDataPorts
	 */
	protected AzureBSConnectionNodeModel() {
		super(new PortType[] {}, new PortType[] { AzureConnectionInformationPortObject.TYPE });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
		return new PortObject[] { new AzureConnectionInformationPortObject(createSpec()) };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		return new PortObjectSpec[] { createSpec() };
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
		m_model.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		m_model.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		m_model.loadValidatedSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// Nothing to do
	}

	/**
	 * Creates the output port specs for this nodeMode.
	 *
	 * @return the ouput port specs
	 * @throws InvalidSettingsException
	 */
	private ConnectionInformationPortObjectSpec createSpec() throws InvalidSettingsException {
		m_model.validateValues();
		final ConnectionInformation connectionInformation = m_model
				.createConnectionInformation(getCredentialsProvider(), AzureBSRemoteFileHandler.PROTOCOL);
		return new ConnectionInformationPortObjectSpec(connectionInformation);
	}
}
