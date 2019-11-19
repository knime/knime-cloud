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
 */
package org.knime.google.cloud.storage.util;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.google.api.data.GoogleApiConnection;
import org.knime.google.cloud.storage.filehandler.GoogleCSRemoteFileHandler;

/**
 * Google cloud connection informations with API connection and project identifier.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCloudStorageConnectionInformation extends CloudConnectionInformation {

    private static final long serialVersionUID = 1L;

    private GoogleApiConnection m_googleApiConnection;

    private static final String CFG_PROJECT = "googleCloudProjectId";
    private String m_project;

    /**
     * Default constructor.
     *
     * @param apiConnection Goolge API connection with credentials
     * @param project Unique Google Cloud project identifier
     */
    public GoogleCloudStorageConnectionInformation(final GoogleApiConnection apiConnection, final String project) {
        m_googleApiConnection = apiConnection;
        m_project = project;
        setProtocol(GoogleCSRemoteFileHandler.PROTOCOL.getName());
        setHost(m_project);
    }

    GoogleCloudStorageConnectionInformation(final ModelContentRO model) throws InvalidSettingsException {
        try {
            m_googleApiConnection = new GoogleApiConnection(model);
            m_project = model.getString(CFG_PROJECT, "");
            setProtocol(GoogleCSRemoteFileHandler.PROTOCOL.getName());
            setHost(m_project);
        } catch (GeneralSecurityException | IOException ex) {
            throw new InvalidSettingsException("Unable to load google api connection: " + ex.getMessage(), ex);
        }
    }

    /**
     * @return Goolge API connection with credentials
     */
    public GoogleApiConnection getGoogleApiConnection() {
        return m_googleApiConnection;
    }

    /**
     * @return Unique Google Cloud project identifier
     */
    public String getProject() {
        return m_project;
    }

    @Override
    public void save(final ModelContentWO model) {
        m_googleApiConnection.save(model);
        model.addString(CFG_PROJECT, m_project);
    }

    /**
     * Load connection informations from stored model.
     */
    public static GoogleCloudStorageConnectionInformation load(final ModelContentRO model) throws InvalidSettingsException {
        return new GoogleCloudStorageConnectionInformation(model);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((m_googleApiConnection == null) ? 0 : m_googleApiConnection.hashCode());
        result = prime * result + ((m_project == null) ? 0 : m_project.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GoogleCloudStorageConnectionInformation other = (GoogleCloudStorageConnectionInformation)obj;
        if (m_googleApiConnection == null) {
            if (other.m_googleApiConnection != null) {
                return false;
            }
        } else if (!m_googleApiConnection.equals(other.m_googleApiConnection)) {
            return false;
        }
        if (m_project == null) {
            if (other.m_project != null) {
                return false;
            }
        } else if (!m_project.equals(other.m_project)) {
            return false;
        }
        return true;
    }
}
