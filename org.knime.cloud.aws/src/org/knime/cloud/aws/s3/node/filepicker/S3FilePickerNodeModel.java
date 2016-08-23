package org.knime.cloud.aws.s3.node.filepicker;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
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
import org.knime.cloud.aws.s3.filehandler.S3RemoteFile;
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

/**
 * This is the model implementation of S3ConnectionToUrl.
 *
 *
 * @author Budi Yanto, KNIME.com
 */
public class S3FilePickerNodeModel extends NodeModel {

	private static NodeLogger LOGGER = NodeLogger.getLogger(S3FilePickerNodeModel.class);

	static final String CFG_EXPIRATION_TIME = "expiration-time";
	static final String CFG_FILE_SELECTION = "file-selection";

	/* adds 1 hour expiration time */
	private static final long DEF_EXPIRATION_TIME = 60 * 60 * 1000;

	/* The name of the output flow variable */
	private static final String FLOW_VARIABLE_NAME = "S3PresignedUrl";

	private final SettingsModelDate m_dateSettingsModel = createExpirationSettingsModel();
	private String m_fileSelection;
	private ConnectionInformation m_connectionInformation;

	/* Create a new SettingsModelDate and initialize it to the current time */
	static SettingsModelDate createExpirationSettingsModel() {
		final SettingsModelDate model = new SettingsModelDate(CFG_EXPIRATION_TIME);
		model.setTimeInMillis(new Date().getTime());
		return model;
	}

	/**
	 * Constructor for the node model.
	 */
	protected S3FilePickerNodeModel() {
		super(new PortType[] { ConnectionInformationPortObject.TYPE }, new PortType[] { FlowVariablePortObject.TYPE });
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
		// Create connection monitor
		final ConnectionMonitor<? extends Connection> monitor = new ConnectionMonitor<>();
		try {
			final URI uri = new URI(m_connectionInformation.toURI().toString() + NodeUtils.encodePath(m_fileSelection));

			// Create remote file for target selection
			final S3RemoteFile remoteFile = (S3RemoteFile) RemoteFileFactory.createRemoteFile(uri,
					m_connectionInformation, monitor);

			remoteFile.open();
			final AmazonS3Client s3Client = remoteFile.getConnection().getClient();
			final String bucketName = remoteFile.getBucketName();
			final String key = remoteFile.getKey();

			final Date expirationTime = m_dateSettingsModel.getDate();

			if (expirationTime.before(new Date())) {
				// Adds default expiration time if the selected time is before
				// the current time
				expirationTime.setTime(new Date().getTime() + DEF_EXPIRATION_TIME);
			}

			LOGGER.debug("Generate Presigned URL with expiration time: " + expirationTime);

			final GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key);
			request.setExpiration(expirationTime);
			final URL url = s3Client.generatePresignedUrl(request);

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

			LOGGER.debug("Push flow variable: " + url.toURI().toString());

			// Push variable
			pushFlowVariableString(name, url.toURI().toString());

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
					|| !m_connectionInformation.getProtocol().equals(AmazonS3.ENDPOINT_PREFIX)) {
				throw new InvalidSettingsException("No S3 connection information available");
			}
		} else {
			throw new InvalidSettingsException("No S3 connection information available");
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
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		settings.addString(CFG_FILE_SELECTION, m_fileSelection);
		m_dateSettingsModel.saveSettingsTo(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_fileSelection = settings.getString(CFG_FILE_SELECTION);
		m_dateSettingsModel.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		settings.getString(CFG_FILE_SELECTION);
		m_dateSettingsModel.validateSettings(settings);
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
