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
 *   Jun 12, 2018 (jtyler): created
 */
package org.knime.cloud.google.drive.filehandler;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.google.util.GoogleDriveConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.TeamDrive;

/**
 * 
 * @author jtyler
 */
public class GoogleDriveRemoteFile extends CloudRemoteFile<GoogleDriveConnection> {
    
    private static final NodeLogger LOGGER = NodeLogger.getLogger(GoogleDriveRemoteFile.class);
    
    private static final String DEFAULT_CONTAINER = "/MyDrive/";
    private static final String TEAM_DRIVES_FOLDER = "/TeamDrives/";
    private static final String GOOGLE_MIME_TYPE = "application/vnd.google-apps";
    private static final String FOLDER = GOOGLE_MIME_TYPE + ".folder";
    private static final String FIELD_STRING = "files(id, name, kind, mimeType, modifiedTime, size, trashed, parents)";
    
    private GoogleDriveRemoteFileMetadata m_fileMetadata;
    
    /**
     * @param uri
     * @param connectionInformation
     * @param connectionMonitor
     * @throws Exception 
     */
    protected GoogleDriveRemoteFile(URI uri, GoogleDriveConnectionInformation connectionInformation,
        ConnectionMonitor<GoogleDriveConnection> connectionMonitor) throws Exception {
        super(uri, connectionInformation, connectionMonitor);
        m_fileMetadata = new GoogleDriveRemoteFileMetadata(uri);
        if (m_fileMetadata.getFileId() == null || m_fileMetadata.getFileId().isEmpty()) {
            m_fileMetadata.setFileId("root");
        }
    }
    
    private Drive getService() throws Exception {
        return getOpenedConnection().getDriveService();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doesContainerExist(String containerName) throws Exception {
        // TODO - clean this up
        //boolean exists = (getContainers().contains("/" + containerName + "/"))  ? true : false;
        //return exists;
        return true;
    }
    
    private List<String> getContainers() throws Exception {
        
        final List<String> validContainerStrings = new ArrayList<String>();
        validContainerStrings.add(DEFAULT_CONTAINER);
        
        final List<TeamDrive> teamDrives = getService().teamdrives().list().execute().getTeamDrives();
        for (TeamDrive teamDrive : teamDrives) {
            validContainerStrings.add(TEAM_DRIVES_FOLDER + teamDrive.getName() + "/");
        }
        
        return validContainerStrings;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getContainerName() throws Exception{
        if (m_containerName == null) {
            if (m_fullPath.equals(DEFAULT_CONTAINER)) {
                m_containerName = "MyDrive";
            } else if (m_fullPath.contentEquals(TEAM_DRIVES_FOLDER)) {
                m_containerName = "TeamDrives";
            } else {
                String[] elements = m_fullPath.split("/");
                if (elements.length < 3 || (elements[0] != null && !elements[0].isEmpty())) {
                    throw new InvalidSettingsException("Invalid path. Container could not be determined.");
                }
                m_containerName = elements[2];
            }
        }
        return m_containerName;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isContainer() throws Exception {
        if (m_isContainer == null) {
            if (m_fullPath.equals(DEFAULT_CONTAINER)) {
                m_isContainer = true;
            } else {
                String[] elements = m_fullPath.split("/");
                if (elements.length == 3 && elements[0].isEmpty() && elements[1].equals("TeamDrives")) {
                    m_isContainer = true;
                } else {
                    m_isContainer = false;
                }
            }
        }
        return m_isContainer;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doestBlobExist(String containerName, String blobName) throws Exception {
        // TODO Auto-generated method stub
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GoogleDriveRemoteFile[] listRootFiles() throws Exception {
        
        GoogleDriveRemoteFile[] rootFiles = new GoogleDriveRemoteFile[2];
        
        // Create remote file for My Drive
        URI uriMyDrive = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
            getURI().getPort(), DEFAULT_CONTAINER, getURI().getQuery(), getURI().getFragment());
        
        rootFiles[0] = new GoogleDriveRemoteFile(uriMyDrive, 
            (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor());
       
        // Create remote file for Team Drives
        URI uriTeamDrive = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
            getURI().getPort(), TEAM_DRIVES_FOLDER, getURI().getQuery(), getURI().getFragment());
        
        rootFiles[1] = new GoogleDriveRemoteFile(uriTeamDrive, 
            (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor());
        
        return rootFiles;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GoogleDriveRemoteFile[] listDirectoryFiles() throws Exception {
        
        LOGGER.info("**** Made it to the file handler : listDirectoryFiles() ****");

        LOGGER.debug("Container name: " + getContainerName());
        LOGGER.debug("Blob name: " + getBlobName());
        if (getURI() != null) {
            LOGGER.debug("Current File URI: " + getURI());
        }
        
        // List the team drives if current file is /TeamDrives/
        if (m_fullPath.equals(TEAM_DRIVES_FOLDER)) {
            final List<GoogleDriveRemoteFile> remoteFiles = new ArrayList<GoogleDriveRemoteFile>();
            final List<TeamDrive> teamDrives = getService().teamdrives().list().execute().getTeamDrives();
            for (TeamDrive teamDrive : teamDrives) {
                final GoogleDriveRemoteFileMetadata metadata = new GoogleDriveRemoteFileMetadata();
                metadata.setFileId("root");
                metadata.setMimeType(FOLDER);
                metadata.setTeamId(teamDrive.getId());
                
                final URI teamURI = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
                    getURI().getPort(), TEAM_DRIVES_FOLDER + teamDrive.getName() + "/", metadata.toQueryString(), getURI().getFragment());
                LOGGER.debug("Team drive URI: " + teamURI);
                remoteFiles.add(new GoogleDriveRemoteFile(teamURI, 
                    (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor()));
            }
            return remoteFiles.toArray(new GoogleDriveRemoteFile[remoteFiles.size()]);
        }
        
        
        // Build File list (with support for team drives if one is set)
        FileList fileList;
        
//        com.google.api.services.drive.Drive.Files.List request = getService().files().list().setQ("'" + m_fileMetadata.getFileId() + "' in parents")
//                .setFields(FIELD_STRING);
        
        if (m_fileMetadata.getTeamId() != null) {
//            request = request.setCorpora("teamDrive").setSupportsTeamDrives(true)
//                    .setIncludeTeamDriveItems(true).setTeamDriveId(m_fileMetadata.getTeamId());
            com.google.api.services.drive.Drive.Files.List request = getService().files().list()
                    .setCorpora("teamDrive").setSupportsTeamDrives(true)
                    .setIncludeTeamDriveItems(true).setTeamDriveId(m_fileMetadata.getTeamId())
                    .setFields(FIELD_STRING);  
            if (!m_fileMetadata.getFileId().equals("root")) {
                request = request.setQ("'" + m_fileMetadata.getFileId() + "' in parents");
            }
            fileList = request.execute();
        } else {
            fileList = getService().files().list().setQ("'" + m_fileMetadata.getFileId() + "' in parents")
                    .setFields(FIELD_STRING).execute();
        }
        
//        FileList fileList = request.execute();
        
        if (fileList == null || fileList.isEmpty()) {
            return new GoogleDriveRemoteFile[0];
        }
        
        List<GoogleDriveRemoteFile> remoteFileList = new ArrayList<GoogleDriveRemoteFile>();
        List<File> driveFiles = fileList.getFiles();
        
        for (File file : driveFiles ) {
            
            String folderPostFix = "";
            
            if (file.getMimeType().equals(FOLDER)) {
                folderPostFix = "/";
                LOGGER.debug("File: " + file.getName() + " is a folder");
            }
                     
            // Set file metadata
            GoogleDriveRemoteFileMetadata metadata = new GoogleDriveRemoteFileMetadata();
            metadata.setFileId(file.getId());
            metadata.setMimeType(file.getMimeType());
            
            if (m_fileMetadata.getTeamId() != null) {
                metadata.setTeamId(m_fileMetadata.getTeamId());
                LOGGER.debug("Team id from file: " + file.getTeamDriveId());
            }
            
            final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
                getURI().getPort(), m_fullPath + file.getName() + folderPostFix, 
                metadata.toQueryString(), getURI().getFragment());
            
            LOGGER.debug("Google Drive Remote URI: " + uri.toString());
            GoogleDriveRemoteFile remoteFile = new GoogleDriveRemoteFile(uri, 
              (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor());
            LOGGER.debug("Remote File Size: " + remoteFile.getBlobSize());
            remoteFileList.add(remoteFile);
        }
        
        return remoteFileList.toArray(new GoogleDriveRemoteFile[remoteFileList.size()]);
    }
    
    private String getFileId(String blobName) throws Exception {
        
        // Get drive's root id
        String rootId = getService().files().get("root").setFields("id").execute().getId();
        
        LOGGER.debug("Blob Name: " + blobName);
        
        final String[] pathElementStringArray = blobName.split("/");
        
        // Build Q-string (We only want to return relevant files/folders in the path to cut down on total
        // files returned)
        String qString = "";
        for (int i = 0; i < pathElementStringArray.length; i++) {
            
            qString += "name = '" + pathElementStringArray[i] + "'";
            
            if (i < pathElementStringArray.length - 1) {
                qString += " or ";
            }
        }
        
        LOGGER.debug("Q-String: ( " + qString + " )");
        
        // Retrieve files that match names in qString
        // This could return file/folder name duplicates, so the next check the the parent is also in the path
        final FileList fileList = getService().files().list().setQ(qString)
                .setFields(FIELD_STRING).execute();
        
        // Use file IDs and parent information to find the right file id
        for (int i = 0; i < pathElementStringArray.length; i++) {
            
            String parent = rootId;
            for (File file : fileList.getFiles()) {
                // If name matches and has the correct parent
                if(file.getName().contentEquals(pathElementStringArray[i]) && file.getParents().contains(parent)) {
                   if (i == pathElementStringArray.length -1) {
                       // Last element, this it the file ID we want
                       return file.getId();
                   } else {
                       // Haven't made it to end of path yet. Set this file ID as new parent
                       parent = file.getId();
                   }
                }
            }
        }
        
        // File ID could not be determined. Likely a bad path given
        throw new Exception("Bad path specified.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getBlobSize() throws Exception {
//        File file = getService().files().get(m_fileMetadata.getFileId()).setFields("size, mimeType").execute();
//        if (!file.getMimeType().contains(GOOGLE_MIME_TYPE) && file.getSize() != null) {
//            return file.getSize();
//        } else {
//            // TODO - Should this method be updated in base class to return Long so we can return null?
//            return -1;
//        }
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getLastModified() throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean deleteContainer() throws Exception {
        throw new UnsupportedOperationException("Deleting Team Drives not supported.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean deleteDirectory() throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean deleteBlob() throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean createContainer() throws Exception {
        throw new UnsupportedOperationException("Team Drive creation not supported.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean createDirectory(String dirName) throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GoogleDriveConnection createConnection() {
        return new GoogleDriveConnection((GoogleDriveConnectionInformation)getConnectionInformation());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI getHadoopFilesystemURI() throws Exception {
        throw new UnsupportedOperationException("Hadoop file system not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream openInputStream() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream openOutputStream() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}