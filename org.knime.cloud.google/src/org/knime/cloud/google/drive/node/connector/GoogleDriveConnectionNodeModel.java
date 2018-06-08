package org.knime.cloud.google.drive.node.connector;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.cloud.google.util.GoogleDriveConnectionInformationPortObject;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.google.api.sheets.data.GoogleSheetsConnection;
import org.knime.google.api.sheets.data.GoogleSheetsConnectionPortObject;


/**
*
* @author Jason Tyler, KNIME.com
*/
public class GoogleDriveConnectionNodeModel extends NodeModel {
	
	// the logger instance
    private static final NodeLogger logger = NodeLogger
            .getLogger(GoogleDriveConnectionNodeModel.class);

    
    public GoogleDriveConnectionNodeModel() {
    	//super(0, 1);
    	super(new PortType[]{GoogleSheetsConnectionPortObject.TYPE},
    			new PortType[] { GoogleDriveConnectionInformationPortObject.TYPE });
    }
    
//	public GoogleDriveConnectionNodeModel(int nrInDataPorts, int nrOutDataPorts) {
//		super(nrInDataPorts, nrOutDataPorts);
//		// TODO Auto-generated constructor stub
//	}
//
//	public GoogleDriveConnectionNodeModel(PortType[] inPortTypes, PortType[] outPortTypes) {
//		super(inPortTypes, outPortTypes);
//	}

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        GoogleSheetsConnection connection =
            ((GoogleSheetsConnectionPortObject)inObjects[0]).getGoogleSheetsConnection();

        exec.setMessage("Requesting Google Sheet");
        exec.setMessage(connection.toString());
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{null};
    }
    
	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		// TODO Auto-generated method stub

	}

	@Override
	protected void reset() {
		// TODO Auto-generated method stub

	}
	
	/**
	 * Create the spec, throw exception if no config available.
	 *
	 * @return ConnectionInformationPortObjectSpec
	 * @throws InvalidSettingsException
	 *             ...
	 */
	private CloudConnectionInformationPortObjectSpec createSpec() throws InvalidSettingsException {
		//m_model.validateValues();
//		final CloudConnectionInformation connectionInformation = m_model
//				.createConnectionInformation(getCredentialsProvider(), S3RemoteFileHandler.PROTOCOL);
//		URI resolve = connectionInformation.toURI().resolve("/");
//        try {
//            RemoteFile<S3Connection> s3RemoteFile = RemoteFileFactory.createRemoteFile(resolve, connectionInformation,
//                new ConnectionMonitor<S3Connection>());
//            S3Connection connection = s3RemoteFile.getConnection();
//            if (connection.restrictedPermissions()) {
//                setWarningMessage("The credentials provided have restricted permissions. "
//                    + "File browsing in the Remote File nodes might not work as expected.\n"
//                    + "All buckets will be assumed existing, as they cannot be listed.");
//            }
//		} catch (InvalidSettingsException ex) {
//		    throw ex;
//		} catch (Exception ex) {
//	        throw new InvalidSettingsException(ex.getMessage());
//		}
//		return new CloudConnectionInformationPortObjectSpec(connectionInformation);
		return null;
	}

}
