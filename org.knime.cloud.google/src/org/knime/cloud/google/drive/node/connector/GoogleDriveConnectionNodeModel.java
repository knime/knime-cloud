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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 *   Jun 11, 2018 (jtyler): created
 */
package org.knime.cloud.google.drive.node.connector;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.FileList;

import java.io.File;
import java.io.IOException;

import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.cloud.google.drive.filehandler.GoogleDriveRemoteFileHandler;
import org.knime.cloud.google.util.GoogleConnectionInformation;
import org.knime.cloud.google.util.GoogleDriveConnectionInformationPortObject;
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
import org.knime.google.api.sheets.data.GoogleSheetsConnection;
import org.knime.google.api.sheets.data.GoogleSheetsConnectionPortObject;
import org.knime.google.api.sheets.data.GoogleSheetsConnectionPortObjectSpec;

/**
 *
 * @author Jason Tyler, KNIME.com
 */
public class GoogleDriveConnectionNodeModel extends NodeModel {
    
    private GoogleDriveConnectorConfiguration m_config = new GoogleDriveConnectorConfiguration();

    /**
     * 
     */
    public GoogleDriveConnectionNodeModel() {
        //super(0, 1);
        super(new PortType[]{GoogleSheetsConnectionPortObject.TYPE},
            new PortType[]{GoogleDriveConnectionInformationPortObject.TYPE});
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

        Drive driveConnection = connection.getDriveService();
        
        getLogger().debug("************");
        getLogger().debug(connection.toString());
        getLogger().debug("***** Drive ******");
        getLogger().debug(driveConnection.toString());
        
        
        FileList fileList = driveConnection.files().list().execute();
        
        getLogger().debug("***** Files found: ******");
        for (com.google.api.services.drive.model.File file: fileList.getFiles()) {
            getLogger().debug("Name:");
            getLogger().debug(file.getName());
            getLogger().debug("Id:");
            getLogger().debug(file.getId());
        }
        
        exec.setMessage("Requesting Google Sheet");
        exec.setMessage(connection.toString());
        //return new PortObject[] {null};
        
        return new PortObject[] { new GoogleDriveConnectionInformationPortObject(createSpec(connection)) };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        GoogleSheetsConnection connection = 
                ((GoogleSheetsConnectionPortObjectSpec)inSpecs[0]).getGoogleSheetsConnection();
        return new PortObjectSpec[] { createSpec(connection)};
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
     * @throws InvalidSettingsException ...
     */
     private CloudConnectionInformationPortObjectSpec createSpec(GoogleSheetsConnection connection) throws InvalidSettingsException {
        //m_model.validateValues();
//        		final CloudConnectionInformation connectionInformation = m_model
//        				.createConnectionInformation(getCredentialsProvider(), S3RemoteFileHandler.PROTOCOL);
//        		URI resolve = connectionInformation.toURI().resolve("/");
//                try {
//                    RemoteFile<S3Connection> s3RemoteFile = RemoteFileFactory.createRemoteFile(resolve, connectionInformation,
//                        new ConnectionMonitor<S3Connection>());
//                    S3Connection connection = s3RemoteFile.getConnection();
//                    if (connection.restrictedPermissions()) {
//                        setWarningMessage("The credentials provided have restricted permissions. "
//                            + "File browsing in the Remote File nodes might not work as expected.\n"
//                            + "All buckets will be assumed existing, as they cannot be listed.");
//                    }
//        		} catch (InvalidSettingsException ex) {
//        		    throw ex;
//        		} catch (Exception ex) {
//        	        throw new InvalidSettingsException(ex.getMessage());
//        		}
        GoogleConnectionInformation connectionInformation = new GoogleConnectionInformation();
        connectionInformation.setConnection(connection);
        
        connectionInformation.setProtocol(GoogleDriveRemoteFileHandler.PROTOCOL.getName());
        connectionInformation.setHost("testhost");
        connectionInformation.setUser("testuser");
        
        return new CloudConnectionInformationPortObjectSpec(connectionInformation);
    }
    

}
