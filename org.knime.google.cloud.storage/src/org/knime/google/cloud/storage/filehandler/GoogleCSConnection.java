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
package org.knime.google.cloud.storage.filehandler;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.google.api.data.GoogleApiConnection;
import org.knime.google.cloud.storage.util.GoogleCloudStorageConnectionInformation;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.storage.Storage;

/**
 * Google Cloud Storage connection.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class GoogleCSConnection extends Connection {

    /** The application name, for which the credentials are stored */
    public static final String APP_NAME = "KNIME-Google-Cloud-Storage-Connector";

	private final GoogleCloudStorageConnectionInformation m_connectionInformation;

	private Storage m_client;

	/**
	 * Default constructor.
	 *
	 * @param connectionInformation
	 */
	public GoogleCSConnection(final GoogleCloudStorageConnectionInformation connectionInformation) {
		m_connectionInformation = connectionInformation;
	}

	@Override
	public void open() throws Exception {
		if (!isOpen()) {
		    m_client = new Storage.Builder(GoogleApiConnection.getHttpTransport(), GoogleApiConnection.getJsonFactory(),
                    m_connectionInformation.getGoogleApiConnection().getCredential()).setApplicationName(APP_NAME).build();
		}
	}

	@Override
	public boolean isOpen() {
		return m_client != null;
	}

	/**
	 * @return the {@link Storage} client for this connection
	 */
	public Storage getClient() {
		return m_client;
	}

	@Override
	public void close() throws Exception {
	    m_client = null;
	}

    /**
     * Generate a signed public URL with expiration time.
     *
     * @param expirationSeconds URL expiration time in seconds from now
     * @param bucketName bucket name
     * @param objectName object name
     * @return signed URL
     * @throws Exception
     */
    protected String getSigningURL(final long expirationSeconds, final String bucketName, final String objectName) throws Exception {
        final GoogleCredential creds =
            (GoogleCredential)m_connectionInformation.getGoogleApiConnection().getCredential();

        return GoogleCSUrlSignature.getSigningURL(creds, expirationSeconds, bucketName, objectName);
    }
}
