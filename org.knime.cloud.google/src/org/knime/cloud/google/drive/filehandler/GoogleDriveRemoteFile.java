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
import java.util.NoSuchElementException;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.google.util.GoogleDriveConnectionInformation;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.TeamDrive;

/**
 * Implementation of {@link CloudRemoteFile} for Google Drive
 * 
 * @author jtyler
 */
public class GoogleDriveRemoteFile extends CloudRemoteFile<GoogleDriveConnection> {
    
    private static final NodeLogger LOGGER = NodeLogger.getLogger(GoogleDriveRemoteFile.class);
    
    private static final String MY_DRIVE = "MyDrive";
    private static final String TEAM_DRIVES = "TeamDrives";
    private static final String DEFAULT_CONTAINER = "/" + MY_DRIVE + "/";
    private static final String TEAM_DRIVES_FOLDER = "/" + TEAM_DRIVES + "/";
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
            // No metadata found in URI (Query params missing). Determine it via API
            m_fileMetadata = getMetadata();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getBlobName() throws Exception {
        if (m_blobName == null) {
            if (isContainer()) {
                return null; 
            }
            if (getContainerName().equals(MY_DRIVE)) {
                m_blobName = getFullPath().substring(DEFAULT_CONTAINER.length());
            } else {
                int idx = StringUtils.ordinalIndexOf(getFullPath(), "/", 3);
                m_blobName = getFullPath().substring(idx + 1);
            }
        }
        return m_blobName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doesContainerExist(String containerName) throws Exception {
        if (containerName.equals(MY_DRIVE) || containerName.equals(TEAM_DRIVES)) {
            return true;
        } else {
            try {
                // Return true if this is a valid Team Drive
                getTeamId(containerName);
                return true;
            } catch (NoSuchElementException ex) {
                return false;
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isContainer() throws Exception {
        if (m_isContainer == null) {
            if (m_fullPath.equals(DEFAULT_CONTAINER) || m_fullPath.equals(TEAM_DRIVES_FOLDER)) {
                m_isContainer = true;
            } else {
                final String[] elements = m_fullPath.split("/");
                if (elements.length == 3 && elements[0].isEmpty() && elements[1].equals(TEAM_DRIVES)) {
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
        // This can get called to verify a file was deleted successfully.
        // So lets reset the metadata
        m_fileMetadata = getMetadata();
        if (m_fileMetadata.getFileId() != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GoogleDriveRemoteFile[] listRootFiles() throws Exception {
        
        final GoogleDriveRemoteFile[] rootFiles = new GoogleDriveRemoteFile[2];
        
        // Create remote file for My Drive
        final URI uriMyDrive = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
            getURI().getPort(), DEFAULT_CONTAINER, getURI().getQuery(), getURI().getFragment());
        
        rootFiles[0] = new GoogleDriveRemoteFile(uriMyDrive, 
            (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor());
       
        // Create remote file for Team Drives
        final URI uriTeamDrive = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
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

        LOGGER.debug("Listing directory files for: " + getFullPath());
        
        // List the team drives if current file path is /TeamDrives/
        if (m_fullPath.equals(TEAM_DRIVES_FOLDER)) {
            final List<GoogleDriveRemoteFile> remoteFiles = new ArrayList<GoogleDriveRemoteFile>();
            final List<TeamDrive> teamDrives = getService().teamdrives().list().execute().getTeamDrives();
            for (TeamDrive teamDrive : teamDrives) {
                final GoogleDriveRemoteFileMetadata metadata = new GoogleDriveRemoteFileMetadata();
                // Use TeamDriveID as File ID for top drive roots
                metadata.setFileId(teamDrive.getId());
                metadata.setMimeType(FOLDER);
                metadata.setTeamId(teamDrive.getId());
                
                final URI teamURI = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
                    getURI().getPort(), TEAM_DRIVES_FOLDER + teamDrive.getName() + "/", 
                    metadata.toQueryString(), getURI().getFragment());
                LOGGER.debug("Team drive URI: " + teamURI);
                remoteFiles.add(new GoogleDriveRemoteFile(teamURI, 
                    (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor()));
            }
            return remoteFiles.toArray(new GoogleDriveRemoteFile[remoteFiles.size()]);
        }
        
        // Build File list (with support for team drives)
        com.google.api.services.drive.Drive.Files.List request = getService().files().list()
                .setQ("'" + m_fileMetadata.getFileId() + "' in parents and trashed = false")
                .setFields(FIELD_STRING);
        
        if (m_fileMetadata.fromTeamDrive()) {
            request = request.setCorpora("teamDrive").setSupportsTeamDrives(true)
                    .setIncludeTeamDriveItems(true).setTeamDriveId(m_fileMetadata.getTeamId());
        } 
        
        List<GoogleDriveRemoteFile> remoteFileList = new ArrayList<GoogleDriveRemoteFile>();
        List<File> driveFiles = request.setOrderBy("name").execute().getFiles();
        
        if (driveFiles == null || driveFiles.isEmpty()) {
            return new GoogleDriveRemoteFile[0];
        }
        
        for (File file : driveFiles ) {
            
            // Google Drive API only allows downloading of non Google file types.
            // (meaning native Google Docs, Spreadsheets, etc. can not be directly downloaded via
            // the API). Let's filter those files out, but keep folder references.
            if (!file.getMimeType().contains(GOOGLE_MIME_TYPE) || file.getMimeType().equals(FOLDER)) {
                String folderPostFix = (file.getMimeType().equals(FOLDER)) ? "/" : "";
                
                // Set file metadata
                GoogleDriveRemoteFileMetadata metadata = new GoogleDriveRemoteFileMetadata();
                metadata.setFileId(file.getId());
                metadata.setMimeType(file.getMimeType());
                if (!file.getMimeType().equals(FOLDER)) {
                    metadata.setFileSize(file.getSize());
                }
                metadata.setLastModified(file.getModifiedTime().getValue() / 1000);
                metadata.setParents(file.getParents());
                
                if (m_fileMetadata.fromTeamDrive()) {
                    metadata.setTeamId(m_fileMetadata.getTeamId());
                }
                
                final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(), 
                    getURI().getPort(), m_fullPath + file.getName() + folderPostFix, 
                    metadata.toQueryString(), getURI().getFragment());
                
                LOGGER.debug("Google Drive Remote URI: " + uri.toString());
                GoogleDriveRemoteFile remoteFile = new GoogleDriveRemoteFile(uri, 
                  (GoogleDriveConnectionInformation)getConnectionInformation(), getConnectionMonitor());
                remoteFileList.add(remoteFile);
            }
        }
        return remoteFileList.toArray(new GoogleDriveRemoteFile[remoteFileList.size()]);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getContainerName() throws Exception{
        if (m_containerName == null) {
            if (getFullPath().equals("/")) {
                m_containerName = null;
            } else if (getFullPath().substring(0, DEFAULT_CONTAINER.length()).equals(DEFAULT_CONTAINER)) {
                m_containerName = MY_DRIVE;
            } else if (getFullPath().equals(TEAM_DRIVES_FOLDER)) {
                m_containerName = TEAM_DRIVES;
            } else {
                String[] elements = getFullPath().split("/");
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
    protected long getBlobSize() throws Exception {
        return m_fileMetadata.getFileSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getLastModified() throws Exception {
        return m_fileMetadata.getLastModified();
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
        // Google Drive API will delete a folder and any subfolders or files by default
        return deleteBlob();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean deleteBlob() throws Exception {
        getService().files().delete(m_fileMetadata.getFileId()).setSupportsTeamDrives(true).execute();
        return true;
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
        
        final File fileMetadata = new File();   
        fileMetadata.setName(getName());
        fileMetadata.setParents(m_fileMetadata.getParents());
        fileMetadata.setMimeType(FOLDER);
     
        try {
            File driveFile = getService().files().create(fileMetadata)
                    .setFields("id, parents, modifiedTime").setSupportsTeamDrives(true).execute();
            
            LOGGER.debug("Creating new folder: " + getBlobName() + " , file id: " + driveFile.getId());
            
            // Set updated metadata (Parents and team ID have already been set)
            m_fileMetadata.setFileId(driveFile.getId());
            m_fileMetadata.setMimeType(FOLDER);
            m_fileMetadata.setLastModified(driveFile.getModifiedTime().getValue() / 1000);
            
            return true;
        } catch (GoogleJsonResponseException ex) {
            LOGGER.debug(ex.getMessage());
            throw new Exception(ex.getStatusMessage());
        }
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
        return getService().files().get(m_fileMetadata.getFileId()).executeMediaAsInputStream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputStream openOutputStream() throws Exception {  
        throw new UnsupportedOperationException("Output Streams not supported for writing to Google Drive.");
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void write(final RemoteFile file, final ExecutionContext exec) throws Exception {
        try (final InputStream in = file.openInputStream()){
            
            final File fileMetadata = new File();   
            fileMetadata.setName(getName());
            fileMetadata.setParents(m_fileMetadata.getParents());
            
            // Media type is null. Google will determine the type based on file
            InputStreamContent content = new InputStreamContent(null, in);
            
            File driveFile = getService().files().create(fileMetadata, content)
                .setFields("id, parents, size, mimeType, modifiedTime").setSupportsTeamDrives(true)
                .execute();
            
            LOGGER.debug("Creating new file: " + getBlobName() + " , file id: " + driveFile.getId());
            
            // Set updated metadata (Parents and team ID have already been set)
            m_fileMetadata.setFileId(driveFile.getId());
            m_fileMetadata.setMimeType(driveFile.getMimeType());
            m_fileMetadata.setFileSize(driveFile.getSize());
            m_fileMetadata.setLastModified(driveFile.getModifiedTime().getValue() / 1000 );
            
        } catch (Exception e) {
            throw e;
        }
    }
    
    private String getTeamId(String teamDriveName) throws Exception {
        if (teamDriveName.equals(MY_DRIVE)) {
            return null;
        }
        final List<TeamDrive> teamDrives = getService().teamdrives().list().execute().getTeamDrives();
        for (TeamDrive teamDrive : teamDrives) {
            if (teamDrive.getName().equals(teamDriveName)) {
                return teamDrive.getId();
            }
        }
        throw new NoSuchElementException("Team Drive: '" + teamDriveName + "' could not be found.");
    }
    
    private GoogleDriveRemoteFileMetadata getMetadata() throws Exception {
        
        GoogleDriveRemoteFileMetadata metadata = new GoogleDriveRemoteFileMetadata();
        String teamId;
        
        // Handle top level folders (MyDrive and TeamDrives)
        if (getFullPath().equals("/")) {
            return metadata;
        }
        if (getFullPath().equals(DEFAULT_CONTAINER) || getFullPath().equals(TEAM_DRIVES_FOLDER)) {
            metadata.setFileId("root");
            return metadata;
        } else {
            teamId = getTeamId(getContainerName());
            if (teamId != null) {
                metadata.setTeamId(teamId);
                // Handle Team drive directory roots
                if (getFullPath().equals(TEAM_DRIVES_FOLDER + getContainerName() + "/")) {
                    metadata.setFileId(teamId);
                    return metadata;
                }
            }
        }
        
        // Set root parent id (id of My Drive or Team Drive)
        String rootId;
        if (teamId == null) {
            rootId = getService().files().get("root").setFields("id").execute().getId();
        } else {
            rootId = teamId;
        }
        
        final String[] pathElementStringArray = getBlobName().split("/");
        
        // Build Q-string (We only want to return relevant files/folders in the path to cut down on total
        // files returned). Ignore "trashed" files
        String qString = "trashed = false and (";
        for (int i = 0; i < pathElementStringArray.length; i++) {
            
            qString += "name = '" + pathElementStringArray[i] + "'";
            
            if (i < pathElementStringArray.length - 1) {
                qString += " or ";
            }
        }
        qString += ")";
        
        LOGGER.debug("Q-String: ( " + qString + " )");
        
        // Handle My Drive and Team Drive files/folders
        com.google.api.services.drive.Drive.Files.List request = getService().files().list();
        if (metadata.fromTeamDrive()) {
            request = request.setCorpora("teamDrive").setSupportsTeamDrives(true)
                    .setIncludeTeamDriveItems(true).setTeamDriveId(metadata.getTeamId());
        }
         
        // Retrieve files that match names in qString
        // This could return file/folder name duplicates, so the next check the the parent is also in the path
        List<File> files = request.setFields(FIELD_STRING)
                .setQ(qString).execute().getFiles();
       
        // Use file IDs and parent information to find the right file id by
        // iterating through each element in the path
        String parent = rootId;
        boolean parentsValidated = false;
        for (int i = 0; i < pathElementStringArray.length; i++) {
            
            boolean fileFound = false;
            
            for (File file : files) {
                // If name matches and has the correct parent
                if(file.getName().equals(pathElementStringArray[i]) && file.getParents().contains(parent)) {
                    fileFound = true;
                    if (i == pathElementStringArray.length -1) {
                       // Last element, this it the file we want
                       metadata.setFileId(file.getId());
                       metadata.setMimeType(file.getMimeType());
                       if (!file.getMimeType().equals(FOLDER)) {
                           metadata.setFileSize(file.getSize());
                       }
                       metadata.setLastModified(file.getModifiedTime().getValue() / 1000);
                       metadata.addParentId(parent);
                       return metadata;
                   } else {
                       // Haven't made it to end of path yet. Set this file ID as new parent
                       parent = file.getId();
                   }
                }
            }
            
            // If the current file was found and we aren't to the end of the blob path,
            // then the parents are still valid.
            // We don't check this for the last file in the array because it could be a 
            // new file being created
            if (fileFound) {
                parentsValidated = true;
            } else if (i < pathElementStringArray.length - 1) {
                parentsValidated = false;
                break;
            }
        }
        
        // In the case that the blob path only has a length of one, the parents are containers and have
        // already been validated. Override parentsValidated to true
        if (pathElementStringArray.length == 1) {
            parentsValidated = true;
        }
        
        // Set the last found location in the path as the parent.
        // This is needed during Google Drive file creation to ensure
        // the file is placed in the correct folder (i.e. its parent)
        if (parentsValidated) {
            metadata.addParentId(parent);
            return metadata; 
        } else {
            // Not all parent directories found, bad path was specified
            throw new NoSuchElementException("Could not resolve all parent locations for path: " + getFullPath());
        }
    }
    
    private Drive getService() throws Exception {
        return getOpenedConnection().getDriveService();
    }

}