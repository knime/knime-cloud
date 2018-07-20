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

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author jtyler
 */
final class GoogleDriveRemoteFileMetadata {

    private String m_fileId;

    private String m_mimeType;

    private String m_teamId;

    private long m_fileSize;

    private long m_lastModified;

    private List<String> m_parents;

    /**
     * Default constructor
     */
    GoogleDriveRemoteFileMetadata() {
    }

    /**
     * Parse Google Drive file metadata from the query parameters set in a URI resource
     * 
     * @param uri
     * @throws Exception
     */
    GoogleDriveRemoteFileMetadata(URI uri) throws Exception {
        if (StringUtils.isNotEmpty(uri.getQuery())) {
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

            // File size
            if (parameterMap.containsKey("size")) {
                m_lastModified = Long.parseLong(parameterMap.get("size"));
            }

            // Last modified
            if (parameterMap.containsKey("modified")) {
                m_lastModified = Long.parseLong(parameterMap.get("modified"));
            }

            // Parents
            if (parameterMap.containsKey("parents")) {
                m_parents = new ArrayList<String>();
                for (String parent : parameterMap.get("parents").split(",")) {
                    m_parents.add(parent);
                }
            }
        }
    }

    /**
     * @return boolean indicating if file is from a team drive
     */
    boolean fromTeamDrive() {
        return (m_teamId != null) ? true : false;
    }

    /**
     * @return fields serialized to a query string
     * @throws Exception
     */
    String toQueryString() throws Exception {

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

        if (m_fileSize != 0l) {
            queryString += "&size=" + m_fileSize;
        }

        if (m_lastModified != 0l) {
            queryString += "&modified=" + m_lastModified;
        }

        if (m_parents != null && m_parents.size() > 0) {
            queryString += "&parents=" + String.join(",", m_parents);
        }

        return queryString;
    }

    /**
     * @return the fileId
     */
    String getFileId() {
        return m_fileId;
    }

    /**
     * @param fileId the fileId to set
     */
    void setFileId(String fileId) {
        m_fileId = fileId;
    }

    /**
     * @return the mimeType
     */
    String getMimeType() {
        return m_mimeType;
    }

    /**
     * @param mimeType the mimeType to set
     */
    void setMimeType(String mimeType) {
        m_mimeType = mimeType;
    }

    /**
     * @return the teamId
     */
    String getTeamId() {
        return m_teamId;
    }

    /**
     * @param teamId the teamId to set
     */
    void setTeamId(String teamId) {
        m_teamId = teamId;
    }

    /**
     * @return List of parent ids
     */
    List<String> getParents() {
        return m_parents;
    }

    /**
     * @param parentId Add a parent ID to the list of parents
     */
    void addParentId(String parentId) {
        // Initialize parent list if it has not been.
        if (m_parents == null) {
            m_parents = new ArrayList<String>();
        }
        m_parents.add(parentId);
    }

    /**
     * @param parents Set all parents via String List
     */
    void setParents(List<String> parents) {
        m_parents = parents;
    }

    /**
     * @return the fileSize
     */
    long getFileSize() {
        return m_fileSize;
    }

    /**
     * @param fileSize the fileSize to set
     */
    void setFileSize(long fileSize) {
        m_fileSize = fileSize;
    }

    /**
     * @return the lastModified
     */
    long getLastModified() {
        return m_lastModified;
    }

    /**
     * @param lastModified the lastModified to set
     */
    void setLastModified(long lastModified) {
        m_lastModified = lastModified;
    }

}
