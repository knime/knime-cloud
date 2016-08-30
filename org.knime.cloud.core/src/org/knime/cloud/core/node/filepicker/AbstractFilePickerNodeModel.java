package org.knime.cloud.core.node.filepicker;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.cloud.core.util.ExpirationSettings;
import org.knime.cloud.core.util.ExpirationSettings.ExpirationMode;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;

/**
 * This is the model implementation of S3ConnectionToUrl.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public abstract class AbstractFilePickerNodeModel extends NodeModel {

	private final ExpirationSettings m_expirationModel = createExpirationSettingsModel();
	private String m_fileSelection;

	private final String m_cfgName;
	private final String m_flowVariableName;

	private ConnectionInformation m_connectionInformation;

	/* Create a new SettingsModelDate and initialize it to the current time */
	static ExpirationSettings createExpirationSettingsModel() {
		final ExpirationSettings model = new ExpirationSettings();
		model.setDateInMillis(new Date().getTime());
		return model;
	}


	/**
	 * Constructor for the node model.
	 */
	protected AbstractFilePickerNodeModel(final String cfgName, final String flowVariableName) {
		super(new PortType[] { ConnectionInformationPortObject.TYPE }, new PortType[] { FlowVariablePortObject.TYPE });
		m_cfgName = cfgName;
		m_flowVariableName = flowVariableName;

	}

	/**
	 * This method returns a signed URL to be passed as flow variable
	 * @return The signed URL
	 * @throws Exception If the URL can not be created
	 */
	abstract protected String getSignedURL(ConnectionMonitor<? extends Connection> monitor, final ConnectionInformation connectionInformation) throws Exception;

	protected Date getExpirationTime() {
		final Date expirationTime = m_expirationModel.getDate();
		if (m_expirationModel.getExpirationMode().equals(ExpirationMode.DURATION.name())) {
			expirationTime.setTime(new Date().getTime() + m_expirationModel.getTimeInMillis());
		}

		return expirationTime;
	}

	/**
	 * Returns the selected file as string
	 * @return the selected file
	 */
	protected String getFileSelection() {
		return m_fileSelection;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		// Create connection monitor
		final ConnectionMonitor<? extends Connection> monitor = new ConnectionMonitor<>();
		try {
			final String url = getSignedURL(monitor, m_connectionInformation);

			final Set<String> variables = getAvailableFlowVariables().keySet();
			String name = m_flowVariableName;
			if (variables.contains(name)) {
				int i = 2;
				name += "_";
				while (variables.contains(name + i)) {
					i++;
				}
				name += i;
			}

			getLogger().debug("Push flow variable: " + url);

			// Push variable
			pushFlowVariableString(name, url);

		} finally {
			monitor.closeAll();
		}

		return new PortObject[] { FlowVariablePortObject.INSTANCE };
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// TODO: generated method stub
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		if (inSpecs[0] != null) {
			final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
			m_connectionInformation = object.getConnectionInformation();
			// Check if the port object has connection information
			if (m_connectionInformation == null
					|| !m_connectionInformation.getProtocol().equals(getEndpointPrefix())) {
				throw new InvalidSettingsException("No " + getEndpointPrefix() +  " connection information available");
			}
		} else {
			throw new InvalidSettingsException("No " + getEndpointPrefix() + "connection information available");
		}
		/*
		 * Validate the selected file. The file cannot be empty, must start with
		 * "/", cannot end with "/" and must have at least two "/" which
		 * indicates that at least the bucket path is defined
		 */
		if (StringUtils.isBlank(m_fileSelection) || !m_fileSelection.startsWith("/") || m_fileSelection.endsWith("/")
				|| m_fileSelection.indexOf("/", 1) < 0) {
			throw new InvalidSettingsException("The selected file is not valid");
		}


		if (m_expirationModel.getExpirationMode().equals(ExpirationMode.DATE.name()) && m_expirationModel.getDate().before(new Date())) {
			throw new InvalidSettingsException("Expiration time: " + m_expirationModel.getDate().toString() + " is in the past. (" + new Date().toString() +")");
		}

		getLogger().debug("Current Time Configure: " + new Date());

		return new PortObjectSpec[] { FlowVariablePortObjectSpec.INSTANCE };
	}




	/**
	 * Return the endpoint's prefix
	 * @return the endpoint's prefix
	 */
	abstract protected String getEndpointPrefix();


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		settings.addString(m_cfgName, m_fileSelection);
		m_expirationModel.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fileSelection = settings.getString(m_cfgName);
		m_expirationModel.loadValidatedSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		settings.getString(m_cfgName);
		m_expirationModel.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO: generated method stub
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File internDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO: generated method stub
	}




}
