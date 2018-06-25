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
 *   Jun 18, 2018 (jtyler): created
 */
package org.knime.cloud.google.drive.filehandler;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * @author jtyler
 */
public class GoogleDriveRemoteFileMetadata {
    
    private String m_fileId;
    private String m_mimeType;
    private String m_teamId;
    private List<String> m_parents = new ArrayList<String>();

    /**
     * 
     */
    public GoogleDriveRemoteFileMetadata() {
        // TODO Auto-generated constructor stub
    }
    
    /**
     * Parse Google Drive file metadata from the query parameters set in a URI resource
     * 
     * @param uri
     * @throws Exception 
     */
    public GoogleDriveRemoteFileMetadata(URI uri) throws Exception {
        if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
            loadFromQueryString(uri.getQuery());
        }
    }
    
    private void loadFromQueryString(String queryString) throws Exception {
        
        Map<String, String> parameterMap = new HashMap<String, String>();
        String[] parameterList = queryString.split("&");
        
        for (String parameter : parameterList) {
            String[] nameValueList = parameter.split("=");
            
            if (nameValueList.length != 2) {
                throw new Exception("Cannot parse query string. Unexpected parameter: " + parameter);
            }
            
            parameterMap.put(nameValueList[0], nameValueList[1]);
            
            // Set required fields
            // File ID
            if (parameterMap.containsKey("fileid")) {
                m_fileId = parameterMap.get("fileid");
            } else {
                throw new Exception("Cannot parse query string. fileid not present.");
            }
            
            // Set optional fields
            // MIME Type
            if (parameterMap.containsKey("fileid")) {
                m_fileId = parameterMap.get("fileid");
            } 
            
            // Team Drive ID
            if (parameterMap.containsKey("teamid")) {
                m_teamId = parameterMap.get("teamid");
            }
        }
    }

    /**
     * @return boolean indicating if file is from a team drive
     */
    public boolean fromTeamDrive() {
        return (m_teamId != null) ? true : false;
    }
    
    /**
     * @return fields serialized to a query string
     * @throws Exception
     */
    public String toQueryString() throws Exception {
        
        if (m_fileId == null) {
            throw new Exception("Cannot create query string. fileid not set.");
        }
        
        String queryString = "fileid=" + m_fileId;
        
        if (m_mimeType != null && !m_mimeType.isEmpty()) {
            queryString += "&mimetype=" + m_mimeType;
        }
        
        if (fromTeamDrive()) {
            queryString += "&teamid=" + m_teamId;
        }
        
        return queryString;
    }
    

    /**
     * @return the fileId
     */
    public String getFileId() {
        return m_fileId;
    }

    /**
     * @param fileId the fileId to set
     */
    public void setFileId(String fileId) {
        m_fileId = fileId;
    }

    /**
     * @return the mimeType
     */
    public String getMimeType() {
        return m_mimeType;
    }

    /**
     * @param mimeType the mimeType to set
     */
    public void setMimeType(String mimeType) {
        m_mimeType = mimeType;
    }

    /**
     * @return the teamId
     */
    public String getTeamId() {
        return m_teamId;
    }

    /**
     * @param teamId the teamId to set
     */
    public void setTeamId(String teamId) {
        m_teamId = teamId;
    }

    /**
     * @return List of parent ids
     */
    public List<String> getParents() {
        return m_parents;
    }

    /**
     * @param parentId Add a parent ID to the list of parents
     */
    public void addParentId(String parentId) {
        m_parents.add(parentId);
    }

}
